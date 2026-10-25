//! Trusted offline executable transport. Pinning authenticates exact bytes; it
//! does not prove lexical semantics or produce rollback compatibility authority.
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            fs::{MetadataExt, OpenOptionsExt},
            process::CommandExt,
        },
    },
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[cfg(feature = "local-lancedb")]
pub mod builder;
#[cfg(feature = "local-lancedb")]
pub mod candidate;
#[cfg(feature = "local-lancedb")]
pub mod evidence;
#[cfg(feature = "local-lancedb")]
pub mod operator;
#[cfg(feature = "local-lancedb")]
pub mod prepared;
#[cfg(feature = "local-lancedb")]
pub mod probe;
#[cfg(feature = "local-lancedb")]
pub mod publication;
#[cfg(feature = "local-lancedb")]
pub mod source;
#[cfg(feature = "local-lancedb")]
pub mod verification;

/// No subprocess output, paths or credentials are included in errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeError;
impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("offline runtime verification failed")
    }
}
impl std::error::Error for RuntimeError {}
impl From<std::io::Error> for RuntimeError {
    fn from(_: std::io::Error) -> Self {
        Self
    }
}
type Result<T> = std::result::Result<T, RuntimeError>;
const MAX_EXECUTABLE: u64 = 1024 * 1024 * 1024;
const MAX_OUTPUT: usize = 1024 * 1024;
const MAX_DURATION: Duration = Duration::from_secs(300);

/// A sealed, owned copy of an explicitly trusted ELF executable. Execution uses
/// these authenticated bytes even if the original path is later replaced.
/// This is deliberately not a verified rollback pair.
pub struct PinnedRuntime {
    executable: File,
    sha256: String,
}
impl PinnedRuntime {
    pub fn pin(path: &Path, expected_sha256: &str, max_bytes: u64) -> Result<Self> {
        if max_bytes == 0
            || max_bytes > MAX_EXECUTABLE
            || expected_sha256.len() != 64
            || !expected_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(RuntimeError);
        }
        let source = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let meta = source.metadata()?;
        // The selected runtime is already trusted by the operator. Do not admit
        // a writable executable from another user or group as that authority.
        let uid = std::fs::metadata("/proc/self")?.uid();
        if !meta.is_file()
            || meta.len() > max_bytes
            || meta.mode() & 0o022 != 0
            || meta.mode() & 0o111 == 0
            || ![0, uid].contains(&meta.uid())
        {
            return Err(RuntimeError);
        }
        // SAFETY: constant terminated name; returned owned fd is checked below.
        let fd = unsafe {
            libc::memfd_create(
                c"nk-offline-runtime".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        if fd < 0 {
            return Err(RuntimeError);
        }
        // SAFETY: fd is newly allocated and has exactly this owner.
        let mut executable = unsafe { File::from_raw_fd(fd) };
        let copied = std::io::copy(&mut source.take(max_bytes + 1), &mut executable)?;
        if copied > max_bytes || copied != meta.len() {
            return Err(RuntimeError);
        }
        executable.seek(SeekFrom::Start(0))?;
        let mut magic = [0; 4];
        executable.read_exact(&mut magic)?;
        if &magic != b"\x7fELF" {
            return Err(RuntimeError);
        }
        executable.seek(SeekFrom::Start(0))?;
        let mut hash = Sha256::new();
        let mut buffer = [0; 65536];
        loop {
            let n = executable.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let sha256 = format!("{:x}", hash.finalize());
        if sha256 != expected_sha256 {
            return Err(RuntimeError);
        }
        // SAFETY: live owned fd; seal forbids any change to authenticated bytes.
        if unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL,
            )
        } < 0
        {
            return Err(RuntimeError);
        }
        Ok(Self { executable, sha256 })
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Run explicit protocol arguments with no inherited environment or stdin.
    /// Both output streams share one byte cap. Nonzero exit, excess output and
    /// deadline expiry fail closed and terminate the whole subprocess group.
    /// Parse/validate the returned bytes separately; successful execution alone
    /// is not evidence that the runtime supports rollback.
    pub fn run(&self, args: &[OsString], timeout: Duration, max_output: usize) -> Result<Vec<u8>> {
        if timeout.is_zero() || timeout > MAX_DURATION || max_output == 0 || max_output > MAX_OUTPUT
        {
            return Err(RuntimeError);
        }
        let started = Instant::now();
        let mut command = Command::new(format!("/proc/self/fd/{}", self.executable.as_raw_fd()));
        command
            .args(args)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = OwnedChild(command.spawn()?);
        let mut stdout = child.0.stdout.take().ok_or(RuntimeError)?;
        let mut stderr = child.0.stderr.take().ok_or(RuntimeError)?;
        for fd in [stdout.as_raw_fd(), stderr.as_raw_fd()] {
            // SAFETY: valid pipe fd; preserve other status flags.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err(RuntimeError);
            }
        }
        let mut output = Vec::new();
        let mut total = 0;
        loop {
            let out_done = drain(&mut stdout, Some(&mut output), &mut total, max_output)?;
            let err_done = drain(&mut stderr, None, &mut total, max_output)?;
            if let Some(success) = exit_success(&child.0)? {
                if !success {
                    return Err(RuntimeError);
                }
                if out_done && err_done {
                    return Ok(output);
                }
            }
            if started.elapsed() >= timeout {
                return Err(RuntimeError);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}
fn drain(
    reader: &mut impl Read,
    mut output: Option<&mut Vec<u8>>,
    total: &mut usize,
    max: usize,
) -> Result<bool> {
    let mut bytes = [0; 8192];
    loop {
        match reader.read(&mut bytes) {
            Ok(0) => return Ok(true),
            Ok(n) => {
                *total = total.checked_add(n).ok_or(RuntimeError)?;
                if *total > max {
                    return Err(RuntimeError);
                }
                if let Some(output) = output.as_mut() {
                    output.extend_from_slice(&bytes[..n]);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(RuntimeError),
        }
    }
}
// Observe termination without reaping the group leader. Its retained PID
// prevents the process-group number being reused before Drop kills descendants.
fn exit_success(child: &Child) -> Result<Option<bool>> {
    // SAFETY: zero is a valid initial siginfo_t and waitid fills it; WNOWAIT
    // keeps this direct child waitable until the owning guard performs wait().
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    if unsafe {
        libc::waitid(
            libc::P_PID,
            child.id(),
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    } < 0
    {
        return Err(RuntimeError);
    }
    // SAFETY: waitid returned successfully; si_pid/status are child-status data.
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    Ok(Some(
        info.si_code == libc::CLD_EXITED && unsafe { info.si_status() } == 0,
    ))
}
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        // SAFETY: process_group(0) gives this child its own group. Also kill
        // descendants retaining a pipe after the immediate child exits.
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.wait();
    }
}

#[cfg(test)]
mod tests;
