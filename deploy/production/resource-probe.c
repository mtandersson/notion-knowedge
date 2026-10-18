/* Credential-free Linux x86_64 fixture only. No libc/runtime dependency. */
#if !defined(__linux__) || !defined(__x86_64__)
#error Resource probe requires Linux x86_64
#endif
static long call(long n, long a, long b, long c, long d) {
    register long r10 __asm__("r10") = d;
    long result;
    __asm__ volatile("syscall" : "=a"(result) : "a"(n), "D"(a), "S"(b), "d"(c), "r"(r10) : "rcx", "r11", "memory");
    return result;
}
static void output(const char *s, long size) { call(1, 1, (long)s, size, 0); }
static char buffer[4096];
static void setting(const char *name, long size, const char *path) {
    long fd = call(257, -100, (long)path, 0, 0);
    if (fd < 0) return;
    long count = call(0, fd, (long)buffer, sizeof buffer, 0);
    call(3, fd, 0, 0, 0);
    if (count > 0) { output(name, size); output(buffer, count); }
}
void _start(void) {
    setting("cpu.max=", 8, "/sys/fs/cgroup/cpu.max");
    setting("memory.max=", 11, "/sys/fs/cgroup/memory.max");
    setting("memory.swap.max=", 16, "/sys/fs/cgroup/memory.swap.max");
    setting("pids.max=", 9, "/sys/fs/cgroup/pids.max");
    long fd = call(257, -100, (long)"/tmp/resource-pressure", 65, 0600);
    if (fd < 0) goto fail;
    long result, written = 0;
    while ((result = call(1, fd, (long)buffer, sizeof buffer, 0)) > 0) {
        written += result;
        if (written > 4 * 1024 * 1024) break;
    }
    call(3, fd, 0, 0, 0);
    call(263, -100, (long)"/tmp/resource-pressure", 0, 0);
    if (result != -28) goto fail; /* ENOSPC */
    output("tmpfs-exhaustion-recovered\n", 27);
    long files[512], count = 0;
    while (count < 512 && (fd = call(257, -100, (long)"/dev/null", 0, 0)) >= 0) files[count++] = fd;
    while (count) call(3, files[--count], 0, 0, 0);
    if (fd != -24) goto fail; /* EMFILE */
    output("fd-exhaustion-recovered\n", 24);
    long children[128], count_children = 0;
    while (count_children < 128) {
        long child = call(57, 0, 0, 0, 0);
        if (child == 0) { call(34, 0, 0, 0, 0); call(60, 1, 0, 0, 0); }
        if (child < 0) { result = child; break; }
        children[count_children++] = child;
    }
    for (long i = 0; i < count_children; i++) call(62, children[i], 9, 0, 0);
    while (count_children) call(61, children[--count_children], 0, 0, 0);
    if (result != -11) goto fail; /* EAGAIN from PID controller */
    output("pid-exhaustion-recovered\n", 25);
    call(60, 0, 0, 0, 0);
fail:
    call(60, 1, 0, 0, 0);
    __builtin_unreachable();
}
