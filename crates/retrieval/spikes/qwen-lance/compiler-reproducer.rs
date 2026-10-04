use std::arch::x86_64::{__m512i, _mm512_dpbusd_epi32};
#[target_feature(enable = "avx512vnni")]
#[unsafe(no_mangle)]
pub unsafe fn qwen_spike_reproducer(a: __m512i, b: __m512i, c: __m512i) -> __m512i {
    _mm512_dpbusd_epi32(a, b, c)
}
fn main() {}
