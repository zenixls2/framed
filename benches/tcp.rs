#![feature(test)]
#![feature(async_closure)]

extern crate test;
use bytes::{BufMut, BytesMut};
use core::pin::Pin;
use framed::tcp::*;
use futures::prelude::*;
use futures::task::{Context, Poll};
use log::*;
use std::net::SocketAddr;
use std::time::Instant;
use test::Bencher;
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio_util::codec::LinesCodec;

struct Server {
    pub(crate) tcp: TcpStream,
    pub(crate) buf: BytesMut,
}
const MSG_NUM: u128 = 100000_u128;

impl Stream for Server {
    type Item = Result<(), ()>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pinned = Pin::get_mut(self);
        let u = match Pin::new(&mut pinned.tcp).poll_write(cx, &pinned.buf) {
            Poll::Ready(Ok(u)) => u,
            Poll::Ready(Err(e)) => {
                println!("{:?}", e);
                return Poll::Ready(Some(Err(())));
            }
            Poll::Pending => return Poll::Pending,
        };
        if u == 0 {
            match Pin::new(&mut pinned.tcp).poll_shutdown(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Ready(Err(e)) => {
                    println!("{:?}", e);
                    Poll::Ready(Some(Err(())))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            let _ = pinned.buf.split_to(u);
            Poll::Ready(Some(Ok(())))
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[bench]
fn test_tcp(_b: &mut Bencher) {
    let mut rt = Runtime::new().unwrap();
    let mut buffer = BytesMut::with_capacity(MSG_NUM as usize * 27);
    for _ in 0..MSG_NUM {
        buffer.put(&b"abcdefghijklmnopqrstuvwxyz\n"[..]);
    }

    let handle = rt.handle().clone();
    rt.spawn(async move {
        let mut listener: TcpListener = TcpListener::bind("0.0.0.0:3996").await.unwrap();
        listener
            .incoming()
            .take(1)
            .for_each(move |result| {
                let server = result.unwrap();
                server.set_nodelay(true).unwrap();
                let s = Server {
                    tcp: server,
                    buf: buffer.clone(),
                };
                handle.spawn(s.for_each(|_| future::ready(())));
                future::ready(())
            })
            .await;
    });

    std::thread::sleep(std::time::Duration::from_micros(100));

    let addr: SocketAddr = "127.0.0.1:3996".parse().unwrap();
    let task = TcpStream::connect(&addr)
        .map_err(|e| error!("{:?}", e))
        .and_then(async move |socket| {
            let frame = TcpFramed::new(socket, LinesCodec::new());
            let now = Instant::now();
            frame
                .for_each(async move |_| ())
                .then(async move |_| {
                    let elapsed = now.elapsed().as_nanos();
                    println!("tcp: {}ns/iter, total: {}ns", elapsed / MSG_NUM, elapsed);
                })
                .await;
            Ok(())
        });
    rt.block_on(task).unwrap();
}

#[bench]
fn test_tcp_old(_b: &mut Bencher) {
    use tokio_util::codec::Framed;
    let mut rt = Runtime::new().unwrap();
    let exec = rt.handle().clone();
    let mut buffer = BytesMut::with_capacity(MSG_NUM as usize * 27);
    for _ in 0..MSG_NUM {
        buffer.put(&b"abcdefghijklmnopqrstuvwxyz\n"[..]);
    }
    rt.spawn(async move {
        let mut listener: TcpListener = TcpListener::bind("0.0.0.0:3996").await.unwrap();
        listener
            .incoming()
            .take(1)
            .for_each(move |result| {
                let server = result.unwrap();
                server.set_nodelay(true).unwrap();
                let s = Server {
                    tcp: server,
                    buf: buffer.clone(),
                };
                exec.spawn(s.for_each(|_| future::ready(())));
                future::ready(())
            })
            .await;
    });
    std::thread::sleep(std::time::Duration::from_micros(100));

    let addr: SocketAddr = "127.0.0.1:3996".parse().unwrap();
    let task = TcpStream::connect(&addr)
        .map_err(|e| error!("{:?}", e))
        .and_then(async move |socket| {
            let frame = Framed::new(socket, LinesCodec::new());
            let now = Instant::now();
            frame
                .for_each(async move |_| ())
                .then(async move |_| {
                    let elapsed = now.elapsed().as_nanos();
                    println!(
                        "tcp old: {}ns/iter, total: {}ns",
                        elapsed / MSG_NUM,
                        elapsed
                    );
                })
                .await;
            Ok(())
        });
    rt.block_on(task).unwrap();
}
