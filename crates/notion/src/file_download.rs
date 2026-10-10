//! Request-scoped temporary file fetching. URL, headers and bodies never enter errors.
use reqwest::{Client, StatusCode, Url};
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadError {
    InvalidPolicy,
    InvalidUrl,
    AddressDenied,
    TooLarge,
    EmptyFile,
    RedirectDenied,
    SourceExpired,
    SourceRejected,
    SourceUnavailable,
    Timeout,
    StorageUnavailable,
}

impl DownloadError {
    /// Retry only with a still-current request URL; never persist it for retries.
    pub fn retryable(self) -> bool {
        matches!(self, Self::SourceUnavailable | Self::Timeout)
    }
}

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidPolicy => "invalid temporary file download policy",
            Self::InvalidUrl => "temporary file URL is not allowed",
            Self::AddressDenied => "temporary file source address is not public",
            Self::TooLarge => "temporary file exceeds the download limit",
            Self::EmptyFile => "temporary file is empty",
            Self::RedirectDenied => "temporary file redirects are not allowed",
            Self::SourceExpired => {
                "temporary file reference expired; supply a fresh file reference"
            }
            Self::SourceRejected => "temporary file source rejected the request",
            Self::SourceUnavailable => {
                "temporary file source unavailable; retry the active request"
            }
            Self::Timeout => "temporary file download timed out; retry the active request",
            Self::StorageUnavailable => "temporary file storage unavailable",
        })
    }
}
impl std::error::Error for DownloadError {}

/// Operator-owned exact hostname allowlist, never derived from tool arguments.
/// This policy authorizes a network destination, not a Notion mutation or root.
#[derive(Debug, Clone)]
pub struct DownloadPolicy {
    hosts: BTreeSet<String>,
    max_bytes: usize,
    deadline: Duration,
}

impl DownloadPolicy {
    pub fn new(
        hosts: &[&str],
        max_bytes: usize,
        deadline: Duration,
    ) -> Result<Self, DownloadError> {
        if hosts.is_empty()
            || hosts.len() > 32
            || max_bytes == 0
            || deadline.is_zero()
            || deadline > Duration::from_secs(120)
        {
            return Err(DownloadError::InvalidPolicy);
        }
        let mut allowed = BTreeSet::new();
        for host in hosts {
            let host = host.to_ascii_lowercase();
            if host.len() > 253
                || !host.contains('.')
                || host.ends_with('.')
                || host.parse::<IpAddr>().is_ok()
                || !host.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'-')
                })
            {
                return Err(DownloadError::InvalidPolicy);
            }
            allowed.insert(host);
        }
        Ok(Self {
            hosts: allowed,
            max_bytes,
            deadline,
        })
    }

    fn url(&self, input: &str) -> Result<Url, DownloadError> {
        if input.len() > 8192
            || input
                .chars()
                .any(|ch| ch.is_control() || ch.is_whitespace())
        {
            return Err(DownloadError::InvalidUrl);
        }
        let url = Url::parse(input).map_err(|_| DownloadError::InvalidUrl)?;
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.port_or_known_default() != Some(443)
            || !url.host_str().is_some_and(|host| self.hosts.contains(host))
        {
            return Err(DownloadError::InvalidUrl);
        }
        Ok(url)
    }

    /// Fetch only for this active request. Fresh DNS is checked and pinned to
    /// the per-request client, preventing a second resolution/rebinding. TLS
    /// still verifies the original hostname. No proxy, cookie jar or credentials.
    pub async fn download(&self, temporary_url: &str) -> Result<DownloadedFile, DownloadError> {
        let url = self.url(temporary_url)?;
        tokio::time::timeout(self.deadline, async {
            let host = url.host_str().ok_or(DownloadError::InvalidUrl)?;
            let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, 443))
                .await
                .map_err(|_| DownloadError::SourceUnavailable)?
                .collect();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !public_address(address.ip()))
            {
                return Err(DownloadError::AddressDenied);
            }
            let client = client_builder(self.deadline)
                .resolve_to_addrs(host, &addresses)
                .build()
                .map_err(|_| DownloadError::SourceUnavailable)?;
            let temporary =
                tempfile::NamedTempFile::new().map_err(|_| DownloadError::StorageUnavailable)?;
            stream_file(&client, url, self.max_bytes, temporary).await
        })
        .await
        .map_err(|_| DownloadError::Timeout)?
    }
}

/// Owns a random private tempfile, unlinked on success-consumer drop, failure or
/// cancellation. Never stores the source URL or upstream filename/header values.
pub struct DownloadedFile {
    file: tempfile::NamedTempFile,
    size_bytes: usize,
}
impl std::fmt::Debug for DownloadedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadedFile")
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}
impl DownloadedFile {
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }
    /// Read the bounded bytes for MIME validation, then upload this same file.
    pub fn file(&self) -> &std::fs::File {
        self.file.as_file()
    }
}

fn request_error(error: reqwest::Error) -> DownloadError {
    if error.is_timeout() {
        DownloadError::Timeout
    } else {
        DownloadError::SourceUnavailable
    }
}

fn client_builder(deadline: Duration) -> reqwest::ClientBuilder {
    Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .connect_timeout(Duration::from_secs(5))
        .timeout(deadline)
}

async fn stream_file(
    client: &Client,
    url: Url,
    max_bytes: usize,
    temporary: tempfile::NamedTempFile,
) -> Result<DownloadedFile, DownloadError> {
    let mut response = client
        .get(url)
        .header("accept-encoding", "identity")
        .send()
        .await
        .map_err(request_error)?;
    let status = response.status();
    if status.is_redirection() {
        return Err(DownloadError::RedirectDenied);
    }
    if matches!(
        status,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::GONE
    ) {
        return Err(DownloadError::SourceExpired);
    }
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(DownloadError::SourceUnavailable);
    }
    // Partial responses must never be mistaken for a complete input file.
    if status != StatusCode::OK || response.headers().contains_key("content-range") {
        return Err(DownloadError::SourceRejected);
    }
    if response
        .headers()
        .get_all("content-encoding")
        .iter()
        .any(|value| value.as_bytes() != b"identity")
    {
        return Err(DownloadError::SourceRejected);
    }
    if response
        .content_length()
        .is_some_and(|size| size > max_bytes as u64)
    {
        return Err(DownloadError::TooLarge);
    }
    let handle = temporary
        .reopen()
        .map_err(|_| DownloadError::StorageUnavailable)?;
    let mut output = tokio::fs::File::from_std(handle);
    let mut size = 0usize;
    while let Some(chunk) = response.chunk().await.map_err(request_error)? {
        size = size
            .checked_add(chunk.len())
            .filter(|size| *size <= max_bytes)
            .ok_or(DownloadError::TooLarge)?;
        output
            .write_all(&chunk)
            .await
            .map_err(|_| DownloadError::StorageUnavailable)?;
    }
    if size == 0 {
        return Err(DownloadError::EmptyFile);
    }
    output
        .flush()
        .await
        .map_err(|_| DownloadError::StorageUnavailable)?;
    drop(output);
    Ok(DownloadedFile {
        file: temporary,
        size_bytes: size,
    })
}

fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            // Global unicast only; exclude special-use, documentation and
            // transition ranges which can encode a non-public IPv4 destination.
            (segments[0] & 0xe000) == 0x2000
                && !(segments[0] == 0x2001 && (segments[1] < 0x200 || segments[1] == 0xdb8))
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && segments[1] < 0x1000)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tokio::io::AsyncReadExt;

    fn policy() -> DownloadPolicy {
        DownloadPolicy::new(&["files.example.test"], 8, Duration::from_secs(1)).unwrap()
    }

    #[test]
    fn exact_https_origins_and_operator_limits_are_required() {
        assert!(
            policy()
                .url("https://files.example.test/file?signature=synthetic")
                .is_ok()
        );
        for url in [
            "http://files.example.test/file",
            "https://files.example.test:8443/file",
            "https://files.example.test.evil.test/file",
            "https://files.example.test@evil.test/file",
            "https://user:pass@files.example.test/file",
            "https://files.example.test/file#fragment",
            "https://127.0.0.1/file",
            "https://files.example.test./file",
            "https://files.example.test/\nfile",
        ] {
            assert_eq!(policy().url(url), Err(DownloadError::InvalidUrl));
        }
        for hosts in [
            &[][..],
            &["localhost"][..],
            &["127.0.0.1"][..],
            &["*.example.test"][..],
            &["bad-.example.test"][..],
        ] {
            assert!(DownloadPolicy::new(hosts, 8, Duration::from_secs(1)).is_err());
        }
        assert!(DownloadPolicy::new(&["files.example.test"], 0, Duration::from_secs(1)).is_err());
        assert!(DownloadPolicy::new(&["files.example.test"], 8, Duration::ZERO).is_err());
    }

    #[test]
    fn private_and_special_use_addresses_cannot_be_pinned() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.1.1",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
            "fe80::1",
            "ff00::1",
            "2001:db8::1",
            "2001::1",
            "2002:7f00:1::",
            "3fff::1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
        for address in [
            "1.1.1.1",
            "8.8.8.8",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(public_address(address.parse().unwrap()), "{address}");
        }
    }

    // Exercise the production client configuration and streaming path with a
    // local HTTP fixture. Only the test overrides HTTPS and public-IP admission;
    // production origin/DNS guards are independently tested above.
    async fn source(
        wire: &'static [u8],
        delay: Duration,
        deadline: Duration,
    ) -> (Client, Url, tokio::task::JoinHandle<()>) {
        source_with_pause(wire, delay, deadline, false).await
    }

    async fn source_with_pause(
        wire: &'static [u8],
        delay: Duration,
        deadline: Duration,
        hold_open: bool,
    ) -> (Client, Url, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0];
                if socket.read(&mut byte).await.unwrap() == 0 {
                    return;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
            assert!(!request.contains("authorization:"));
            assert!(request.contains("accept-encoding: identity"));
            tokio::time::sleep(delay).await;
            let _ = socket.write_all(wire).await;
            if hold_open {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
        let client = client_builder(deadline)
            .https_only(false)
            .resolve_to_addrs("files.example.test", &[address])
            .build()
            .unwrap();
        let url = Url::parse(&format!(
            "http://files.example.test:{}/file?signature=private-sentinel",
            address.port()
        ))
        .unwrap();
        (client, url, task)
    }

    #[tokio::test]
    async fn streams_exact_limit_to_private_owned_file_then_removes_it() {
        let directory = tempfile::tempdir().unwrap();
        let temporary = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
        let path = temporary.path().to_owned();
        let (client, url, task) = source(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n", Duration::ZERO, Duration::from_secs(1)).await;
        let download = stream_file(&client, url, 8, temporary).await.unwrap();
        assert_eq!(download.size_bytes(), 8);
        let mut bytes = Vec::new();
        download.file().read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"12345678");
        assert!(!format!("{download:?}").contains("private-sentinel"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        drop(download);
        assert!(!path.exists());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_redirects_sizes_partial_or_encoded_files_and_classifies_retry() {
        for (wire, expected) in [
            (&b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/private?token=sentinel\r\nContent-Length: 0\r\n\r\n"[..], DownloadError::RedirectDenied),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n123456789"[..], DownloadError::TooLarge),
            (&b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n9\r\n123456789\r\n0\r\n\r\n"[..], DownloadError::TooLarge),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"[..], DownloadError::EmptyFile),
            (&b"HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\n\r\nx"[..], DownloadError::SourceRejected),
            (&b"HTTP/1.1 200 OK\r\nContent-Range: bytes 0-0/10\r\nContent-Length: 1\r\n\r\nx"[..], DownloadError::SourceRejected),
            (&b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 1\r\n\r\nx"[..], DownloadError::SourceRejected),
            (&b"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\nContent-Encoding: gzip\r\nContent-Length: 1\r\n\r\nx"[..], DownloadError::SourceRejected),
            (&b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"[..], DownloadError::SourceExpired),
            (&b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n"[..], DownloadError::SourceUnavailable),
            (&b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"[..], DownloadError::SourceUnavailable),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n1234"[..], DownloadError::SourceUnavailable),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let temporary = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
            let path = temporary.path().to_owned();
            let (client, url, task) = source(wire, Duration::ZERO, Duration::from_secs(1)).await;
            let error = stream_file(&client, url, 8, temporary).await.unwrap_err();
            assert_eq!(error, expected);
            assert!(!format!("{error:?} {error}").contains("sentinel"));
            assert!(!path.exists());
            task.await.unwrap();
        }
        assert!(DownloadError::SourceUnavailable.retryable());
        assert!(DownloadError::Timeout.retryable());
        assert!(!DownloadError::SourceExpired.retryable());
    }

    #[tokio::test]
    async fn timeout_and_caller_cancellation_remove_temporary_bytes() {
        for cancel in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let temporary = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
            let path = temporary.path().to_owned();
            let (client, url, server) = source_with_pause(
                b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\ndata",
                Duration::ZERO,
                Duration::from_millis(500),
                true,
            )
            .await;
            let task = tokio::spawn(async move { stream_file(&client, url, 8, temporary).await });
            tokio::time::timeout(Duration::from_millis(400), async {
                loop {
                    if std::fs::metadata(&path).unwrap().len() == 4 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(task.await.unwrap().unwrap_err(), DownloadError::Timeout);
            }
            assert!(!path.exists());
            server.abort();
        }
    }
}
