use divan::{Bencher, black_box};
use fastalp::{compress, compress_into, decompress_into};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
  divan::main();
}

fn generate_sensor_data(count: usize) -> Vec<f64> {
  (0..count).map(|i| (200 + (i % 150)) as f64 * 0.1).collect()
}

fn generate_sensor_data_f32(count: usize) -> Vec<f32> {
  (0..count)
    .map(|i| (200 + (i % 150)) as f32 * 0.1f32)
    .collect()
}

fn generate_random_data(count: usize) -> Vec<f64> {
  fastrand::seed(42);
  (0..count)
    .map(|_| {
      let base = fastrand::i32(-1000..1000) as f64;
      let dec = fastrand::u32(0..1000) as f64 * 0.01;
      base + dec
    })
    .collect()
}

// ───────────────────────────────────────────────
// 1. f64 压缩与解压基准测试 (1024 浮点数，标准向量大小)
// ───────────────────────────────────────────────

#[divan::bench]
fn bench_compress_f64_sensor_1024(bencher: Bencher) {
  let data = generate_sensor_data(1024);
  let mut dst = Vec::with_capacity(data.len() * 2 + 16);
  bencher.bench_local(|| {
    dst.clear();
    compress_into(&data, &mut dst);
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_decompress_f64_sensor_1024(bencher: Bencher) {
  let data = generate_sensor_data(1024);
  let compressed = compress(&data);
  let mut dst: Vec<f64> = Vec::with_capacity(1024);
  bencher.bench_local(|| {
    dst.clear();
    decompress_into(&compressed, &mut dst).unwrap();
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_compress_f64_random_1024(bencher: Bencher) {
  let data = generate_random_data(1024);
  let mut dst = Vec::with_capacity(data.len() * 2 + 16);
  bencher.bench_local(|| {
    dst.clear();
    compress_into(&data, &mut dst);
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_decompress_f64_random_1024(bencher: Bencher) {
  let data = generate_random_data(1024);
  let compressed = compress(&data);
  let mut dst: Vec<f64> = Vec::with_capacity(1024);
  bencher.bench_local(|| {
    dst.clear();
    decompress_into(&compressed, &mut dst).unwrap();
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_compress_f64_identical_1024(bencher: Bencher) {
  let data = vec![98.6f64; 1024];
  let mut dst = Vec::with_capacity(64);
  bencher.bench_local(|| {
    dst.clear();
    compress_into(&data, &mut dst);
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_decompress_f64_identical_1024(bencher: Bencher) {
  let data = vec![98.6f64; 1024];
  let compressed = compress(&data);
  let mut dst: Vec<f64> = Vec::with_capacity(1024);
  bencher.bench_local(|| {
    dst.clear();
    decompress_into(&compressed, &mut dst).unwrap();
    black_box(&dst);
  });
}

// ───────────────────────────────────────────────
// 2. f32 压缩与解压基准测试 (1024 浮点数)
// ───────────────────────────────────────────────

#[divan::bench]
fn bench_compress_f32_sensor_1024(bencher: Bencher) {
  let data = generate_sensor_data_f32(1024);
  let mut dst = Vec::with_capacity(data.len() * 2 + 16);
  bencher.bench_local(|| {
    dst.clear();
    compress_into(&data, &mut dst);
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_decompress_f32_sensor_1024(bencher: Bencher) {
  let data = generate_sensor_data_f32(1024);
  let compressed = compress(&data);
  let mut dst: Vec<f32> = Vec::with_capacity(1024);
  bencher.bench_local(|| {
    dst.clear();
    decompress_into(&compressed, &mut dst).unwrap();
    black_box(&dst);
  });
}

// ───────────────────────────────────────────────
// 3. 大块批量压缩与解压吞吐测试 (65536 浮点数，512 KB 数据)
// ───────────────────────────────────────────────

#[divan::bench]
fn bench_compress_f64_large_batch(bencher: Bencher) {
  let data = generate_sensor_data(65535);
  let mut dst = Vec::with_capacity(data.len() * 2 + 16);
  bencher.bench_local(|| {
    dst.clear();
    compress_into(&data, &mut dst);
    black_box(&dst);
  });
}

#[divan::bench]
fn bench_decompress_f64_large_batch(bencher: Bencher) {
  let data = generate_sensor_data(65535);
  let compressed = compress(&data);
  let mut dst: Vec<f64> = Vec::with_capacity(65535);
  bencher.bench_local(|| {
    dst.clear();
    decompress_into(&compressed, &mut dst).unwrap();
    black_box(&dst);
  });
}
