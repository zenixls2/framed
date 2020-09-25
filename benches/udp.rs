#![feature(test)]
#![feature(async_closure)]

extern crate test;
use bytes::{BufMut, BytesMut};
use core::pin::Pin;
use framed::udp::*;
use futures::future;
use futures::prelude::*;
use futures::task::{Context, Poll};
use net2::UdpBuilder;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use test::Bencher;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::time::delay_for;
use tokio_util::codec::LinesCodec;

struct Server {
    pub(crate) udp: UdpSocket,
    pub(crate) buf: BytesMut,
    pub(crate) addr: SocketAddr,
    pub(crate) size: usize,
    pub(crate) nanos: u64,
}

const MSG_NUM: u128 = 100000_u128;

impl Stream for Server {
    type Item = Result<(), ()>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pinned = Pin::get_mut(self);
        let mut len = pinned.buf.len();
        if len == 0 {
            println!("eof, exit");
            return Poll::Ready(None);
        }

        len = std::cmp::min(len, pinned.size);
        let buf = pinned.buf.split_to(len);
        sleep(Duration::from_nanos(pinned.nanos));

        match pinned.udp.poll_send_to(cx, &buf[0..len], &pinned.addr) {
            Poll::Ready(Ok(u)) => {
                assert_eq!(u, len);
                Poll::Ready(Some(Ok(())))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                println!("{:?}", e);
                Poll::Ready(Some(Err(())))
            }
        }
    }
}

#[bench]
fn test_udp(_b: &mut Bencher) {
    let mut rt = Runtime::new().unwrap();
    let multiaddr = "235.55.55.55".parse().unwrap();
    let interface = "0.0.0.0".parse().unwrap();
    let multisock = SocketAddr::new(IpAddr::V4(multiaddr), 3997_u16);
    let mut buffer = BytesMut::with_capacity(MSG_NUM as usize * 27);
    for _ in 0..MSG_NUM {
        buffer.put(&b"abcdefghijklmnopqrstuvwxyz\n"[..]);
    }

    let builder = UdpBuilder::new_v4().unwrap();
    builder.reuse_address(true).unwrap();
    let socket = builder.bind(&multisock).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter1 = Arc::clone(&counter);
    let framed = rt.enter(move || {
        let udp = UdpSocket::from_std(socket).unwrap();
        udp.join_multicast_v4(multiaddr, interface).unwrap();
        UdpFramed::new(udp, LinesCodec::new())
    });
    let shutdown = framed.get_shutdown();
    rt.spawn(async move {
        let builder = UdpBuilder::new_v4().unwrap();
        builder.reuse_address(true).unwrap();
        let socket = builder.bind(&multisock).unwrap();
        let udp = UdpSocket::from_std(socket).unwrap();
        udp.join_multicast_v4(multiaddr, interface).unwrap();
        let server = Server {
            udp: udp,
            buf: buffer.clone(),
            addr: multisock,
            size: 4050,
            nanos: 1,
        };
        let delay = delay_for(Duration::from_secs(1));
        delay.await;
        server.for_each(|_| future::ready(())).await;
        shutdown.shutdown();
    });
    rt.block_on(async move {
        let now = Instant::now();
        framed
            .map_err(|_| ())
            .take(MSG_NUM as usize)
            .for_each(move |result| {
                counter.fetch_add(1, SeqCst);
                let i = result.unwrap();
                assert_eq!(i.0.len(), 26);
                future::ready(())
            })
            .await;
        let elapsed = now.elapsed().as_nanos();
        println!(
            "new - all: {}, udp: {}ns/iter, total: {}ns",
            counter1.load(Relaxed),
            elapsed / MSG_NUM,
            elapsed
        );
    });
}

#[bench]
fn test_udp_old(_b: &mut Bencher) {
    use tokio_util;
    let mut rt = Runtime::new().unwrap();
    let multiaddr = "235.55.55.55".parse().unwrap();
    let interface = "0.0.0.0".parse().unwrap();
    let multisock = SocketAddr::new(IpAddr::V4(multiaddr), 3998_u16);
    let mut buffer = BytesMut::with_capacity(MSG_NUM as usize * 27);
    for _ in 0..MSG_NUM {
        buffer.put(&b"abcdefghijklmnopqrstuvwxyz\n"[..]);
    }

    let builder = UdpBuilder::new_v4().unwrap();
    builder.reuse_address(true).unwrap();
    let socket = builder.bind(&multisock).unwrap();
    rt.spawn(async move {
        let udp = UdpSocket::from_std(socket).unwrap();
        udp.join_multicast_v4(multiaddr, interface).unwrap();
        let server = Server {
            udp: udp,
            buf: buffer.clone(),
            addr: multisock,
            // UdpFramed will take the first part and drop the remaining message.
            size: 27,
            nanos: 1,
        };
        let delay = delay_for(Duration::from_secs(1));
        delay.await;
        server.for_each(|_| future::ready(())).await;
    });

    let builder = UdpBuilder::new_v4().unwrap();
    builder.reuse_address(true).unwrap();
    let socket = builder.bind(&multisock).unwrap();
    rt.block_on(async move {
        let udp = UdpSocket::from_std(socket).unwrap();
        udp.join_multicast_v4(multiaddr, interface).unwrap();
        let framed = tokio_util::udp::UdpFramed::new(udp, LinesCodec::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = Arc::clone(&counter);
        let now = Instant::now();
        framed
            .map_err(|_| ())
            .take(MSG_NUM as usize)
            .for_each(move |result| {
                counter.fetch_add(1, SeqCst);
                let i = result.unwrap();
                assert_eq!(i.0.len(), 26);
                future::ready(())
            })
            .await;
        let elapsed = now.elapsed().as_nanos();
        println!(
            "old - all: {}, udp: {}ns/iter, total: {}ns",
            counter1.load(Relaxed),
            elapsed / MSG_NUM,
            elapsed
        );
    });
}
