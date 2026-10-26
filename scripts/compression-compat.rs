//! Cross-version persisted-format probe, driven by test-compression-compat.py.
use std::{fs, io::Cursor, path::Path, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{
    CompressionType,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use lance_encoding::encodings::physical::block::{
    CompressionConfig, CompressionScheme, GeneralBufferCompressor,
};

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let ids = Int64Array::from_iter((0..4096).map(|i| (i % 11 != 0).then_some(i)));
    let texts = StringArray::from_iter((0..4096).map(|i| {
        (i % 13 != 0).then_some(if i % 2 == 0 {
            "åäö 日本語"
        } else {
            "compression compatibility"
        })
    }));
    RecordBatch::try_new(schema, vec![Arc::new(ids) as ArrayRef, Arc::new(texts)]).unwrap()
}

fn payloads() -> Vec<Vec<u8>> {
    vec![
        vec![],
        b"a".to_vec(),
        "repeated åäö 日本語 text\n".repeat(4096).into_bytes(),
        (0u32..32768)
            .flat_map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes())
            .collect(),
    ]
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(args.len(), 3);
    let directory = Path::new(&args[2]);
    let expected = batch();
    let compressor = GeneralBufferCompressor::get_compressor(CompressionConfig::new(
        CompressionScheme::Zstd,
        Some(3),
    ))
    .unwrap();
    match args[1].as_str() {
        "write" => {
            fs::create_dir_all(directory).unwrap();
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut writer = FileWriter::try_new_with_options(
                fs::File::create(directory.join("records.arrow")).unwrap(),
                expected.schema().as_ref(),
                options,
            )
            .unwrap();
            // Multiple batches exercise the actual Arrow compressor's reused context.
            for _ in 0..3 {
                writer.write(&expected).unwrap();
            }
            writer.finish().unwrap();
            for (i, payload) in payloads().iter().enumerate() {
                let mut encoded = Vec::new();
                // Reuse Lance's compressor across buffers of different sizes.
                compressor.compress(payload, &mut encoded).unwrap();
                assert_eq!(&encoded[..8], &(payload.len() as u64).to_le_bytes());
                fs::write(directory.join(format!("block-{i}.lance-zstd")), encoded).unwrap();
                // Lance also reads legacy raw streaming frames (without its length prefix).
                let raw = zstd::stream::encode_all(Cursor::new(payload), 3).unwrap();
                fs::write(directory.join(format!("block-{i}.raw-zstd")), raw).unwrap();
            }
        }
        "read" => {
            let reader = FileReader::try_new(
                fs::File::open(directory.join("records.arrow")).unwrap(),
                None,
            )
            .unwrap();
            let batches: Vec<_> = reader.map(Result::unwrap).collect();
            assert_eq!(batches.len(), 3);
            for actual in batches {
                assert_eq!(actual, expected);
            }
            for (i, payload) in payloads().iter().enumerate() {
                for extension in ["lance-zstd", "raw-zstd"] {
                    let encoded =
                        fs::read(directory.join(format!("block-{i}.{extension}"))).unwrap();
                    let mut decoded = b"existing prefix".to_vec();
                    compressor.decompress(&encoded, &mut decoded).unwrap();
                    assert_eq!(&decoded[..15], b"existing prefix");
                    assert_eq!(&decoded[15..], payload);
                }
            }
        }
        _ => panic!("expected write or read"),
    }
    println!("{} passed: {}", args[1], directory.display());
}
