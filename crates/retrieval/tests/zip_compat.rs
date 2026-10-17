//! Preserve Candle NPZ IO while requiring patched ZIP filename validation.
#![cfg(feature = "local-qwen")]
use candle_core::{Device, Tensor};

#[test]
fn candle_npz_roundtrip_preserves_names_shapes_and_values() {
    let directory = std::env::temp_dir().join(format!(
        "nk-zip-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("weights.npz");
    let tensor = Tensor::from_slice(&[1_f32, 2., 3., 4.], (2, 2), &Device::Cpu).unwrap();
    Tensor::write_npz(&[("layer.weights", &tensor)], &path).unwrap();
    let tensors = Tensor::read_npz(&path).unwrap();
    assert_eq!(tensors.len(), 1);
    assert_eq!(tensors[0].0, "layer.weights");
    assert_eq!(tensors[0].1.dims(), &[2, 2]);
    assert_eq!(
        tensors[0].1.to_vec2::<f32>().unwrap(),
        vec![vec![1., 2.], vec![3., 4.]]
    );
    let lazy = candle_core::npy::NpzTensors::new(&path).unwrap();
    assert_eq!(lazy.get("layer.weights").unwrap().unwrap().dims(), &[2, 2]);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn candle_zip9_decodes_utf8_and_legacy_cp437_tensor_names_and_rejects_invalid_archives() {
    let directory = std::env::temp_dir().join(format!(
        "nk-zip-names-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("names.npz");
    let tensor = Tensor::new(&[2_u32, 4, 6], &Device::Cpu).unwrap();
    for name in ["données", "a"] {
        Tensor::write_npz(&[(name, &tensor)], &path).unwrap();
        let expected = if name == "a" {
            let mut bytes = std::fs::read(&path).unwrap();
            let positions: Vec<_> = bytes
                .windows(5)
                .enumerate()
                .filter_map(|(i, value)| (value == b"a.npy").then_some(i))
                .collect();
            assert_eq!(positions.len(), 2);
            for i in positions {
                bytes[i] = 0x82;
            }
            std::fs::write(&path, bytes).unwrap();
            "é"
        } else {
            name
        };
        let values = Tensor::read_npz(&path).unwrap();
        assert_eq!(values[0].0, expected);
        assert_eq!(values[0].1.to_vec1::<u32>().unwrap(), vec![2, 4, 6]);
        let lazy = candle_core::npy::NpzTensors::new(&path).unwrap();
        assert_eq!(
            lazy.get(expected)
                .unwrap()
                .unwrap()
                .to_vec1::<u32>()
                .unwrap(),
            vec![2, 4, 6]
        );
    }
    std::fs::write(&path, b"not a zip archive").unwrap();
    assert!(matches!(
        Tensor::read_npz(&path),
        Err(candle_core::Error::Zip(_))
    ));
    assert!(matches!(
        candle_core::npy::NpzTensors::new(&path),
        Err(candle_core::Error::Zip(_))
    ));
    assert!(matches!(
        candle_core::pickle::read_pth_tensor_info(&path, false, None),
        Err(candle_core::Error::Zip(_))
    ));
    std::fs::remove_dir_all(directory).unwrap();
}
