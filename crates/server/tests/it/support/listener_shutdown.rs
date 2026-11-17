//! Observe release of the listening socket without adding accepted connections
//! to the process being drained. Accepted connections can remain alive.
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::net::TcpSocket;

fn listener_released(port: u16) -> io::Result<bool> {
    assert_ne!(port, 0, "observe the actual bound port");
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    match socket.bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port))) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) async fn wait_listener_closed(port: u16) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !listener_released(port).expect("listener release must be observable") {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("listener must close before the existing shutdown deadline");
}

#[tokio::test]
async fn release_probe_requires_listener_drop_and_preserves_accepted_connection() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut client = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    let (mut accepted, _) = listener.accept().await.unwrap();
    assert!(!listener_released(port).unwrap());
    drop(listener);
    wait_listener_closed(port).await;
    client.write_all(b"still draining").await.unwrap();
    let mut bytes = [0u8; 14];
    accepted.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still draining");
}
