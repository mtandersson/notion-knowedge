/* Read-only Linux x86_64 sampler for an owned disposable runtime container.
 * No libc, source contents, process environment, network, allocation or writes.
 * Counts allocated filesystem bytes; traversal stops at 100000 entries/depth32.
 */
#if !defined(__linux__) || !defined(__x86_64__)
#error Measurement probe requires Linux x86_64
#endif
static long syscall4(long n, long a, long b, long c, long d) {
    register long r10 __asm__("r10") = d;
    long r;
    __asm__ volatile("syscall" : "=a"(r) : "a"(n), "D"(a), "S"(b), "d"(c), "r"(r10) : "rcx", "r11", "memory");
    return r;
}
static long length(const char *s) { long n=0; while(s[n]) n++; return n; }
static void out(const char *s) { syscall4(1,1,(long)s,length(s),0); }
static void number(unsigned long n) { char b[32]; long i=31; b[i]=0; do { b[--i]='0'+n%10;n/=10; } while(n);out(b+i); }
static int failed;
static void file(const char *key,const char *path) {
    char b[8192];long f=syscall4(257,-100,(long)path,0,0);
    if(f<0){failed=1;return;}
    long n=syscall4(0,f,(long)b,sizeof b-1,0);syscall4(3,f,0,0,0);
    if(n<0||n>=sizeof b-1){failed=1;return;}b[n]=0;
    out(key);out("\n");out(b);out("\nEND\n");
}
struct entry { unsigned long ino; long off; unsigned short len; unsigned char type; char name[]; };
struct status { unsigned long dev,ino,nlink;unsigned int mode,uid,gid,pad;unsigned long rdev;long size,blksize,blocks;long rest[9]; };
static unsigned long visited;
static unsigned long tree(const char *path,int depth,int count_only) {
    if(depth>32){failed=1;return 0;}
    long fd=syscall4(257,-100,(long)path,65536|131072,0);
    if(fd<0){failed=1;return 0;}
    unsigned long total=0;char buf[4096];long n;
    while((n=syscall4(217,fd,(long)buf,sizeof buf,0))>0) {
        for(long p=0;p<n;) {
            struct entry *e=(void*)(buf+p);
            if(e->len<20||p+e->len>n){failed=1;break;}
            p+=e->len;
            if(e->name[0]=='.'&&(!e->name[1]||(e->name[1]=='.'&&!e->name[2])))continue;
            if(++visited>100000){failed=1;break;}
            if(count_only){total++;continue;}
            char child[4096];long a=length(path),b=length(e->name);
            if(a+b+2>sizeof child){failed=1;break;}
            for(long i=0;i<a;i++)child[i]=path[i];child[a]='/';
            for(long i=0;i<=b;i++)child[a+1+i]=e->name[i];
            struct status st;
            if(syscall4(262,-100,(long)child,(long)&st,256)<0){failed=1;break;}
            total+=(unsigned long)st.blocks*512;
            if((st.mode&0170000)==0040000)total+=tree(child,depth+1,0);
            if(failed)break;
        }
        if(failed)break;
    }
    if(n<0)failed=1;syscall4(3,fd,0,0,0);return total;
}
static void metric(const char *key,const char *path,int count) {
    visited=0;unsigned long n=tree(path,0,count);out(key);out("\n");number(n);out("\nEND\n");
}
void _start(void) {
    file("cpu.max","/sys/fs/cgroup/cpu.max");
    file("cpu.stat","/sys/fs/cgroup/cpu.stat");
    file("memory.max","/sys/fs/cgroup/memory.max");
    file("memory.current","/sys/fs/cgroup/memory.current");
    file("memory.peak","/sys/fs/cgroup/memory.peak");
    file("memory.events","/sys/fs/cgroup/memory.events");
    file("memory.swap.max","/sys/fs/cgroup/memory.swap.max");
    file("pids.max","/sys/fs/cgroup/pids.max");
    file("pids.current","/sys/fs/cgroup/pids.current");
    /* status contains bounded process counters/name, never cmdline/environment. */
    file("process.status","/proc/1/status");
    file("process.limits","/proc/1/limits");
    metric("process.fd_count","/proc/1/fd",1);
    metric("tmp.allocated_bytes","/tmp",0);
    metric("index.allocated_bytes","/var/lib/notion-knowledge/index",0);
    metric("state.allocated_bytes","/var/lib/notion-knowledge/state",0);
    syscall4(60,failed?1:0,0,0,0);__builtin_unreachable();
}
