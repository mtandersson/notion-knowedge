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
