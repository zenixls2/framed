use crate::shutdown::Shutdown;
use bytes::{BufMut, BytesMut};
use core::pin::Pin;
use core::task::Waker;
use futures::prelude::*;
use futures::task::{Context, Poll};
use std::io;
use std::marker::Unpin;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;
use tokio_util::codec::{Decoder, Encoder};

#[must_use = "sinks do nothing unless polled"]
pub struct UdpFramed<C> {
    socket: UdpSocket,
    codec: C,
    rd: BytesMut,
    wr: BytesMut,
    out_addr: SocketAddr,
    flushed: bool,
    shutdown: Arc<AtomicBool>,
    // task will be saved here before NotReady
    current: Arc<Mutex<Option<Waker>>>,
    // flag to check whether to do poll_read on socket
    is_ready: bool,
    is_end: bool,
    tmp_addr: Option<SocketAddr>,
}

impl<C: Unpin + Decoder> Stream for UdpFramed<C> {
    type Item = Result<(C::Item, SocketAddr), C::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let pinned = Pin::get_mut(self);
        if pinned.is_ready {
            loop {
                // make sure all buffer consumed before poll_recv_from
                // tmp_addr will still be the same
                match pinned.codec.decode(&mut pinned.rd) {
                    Ok(None) => {}
                    Ok(Some(e)) => return Poll::Ready(Some(Ok((e, pinned.tmp_addr.unwrap())))),
                    Err(e) => {
                        pinned.is_end = true;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                };
                pinned.rd.reserve(INITIAL_RD_CAPACITY);
                unsafe {
                    // Read into the buffer without having to initialize the memory.
                    let bytes = &mut *(pinned.rd.bytes_mut() as *mut _ as *mut [u8]);
                    match Pin::new(&mut pinned.socket).poll_recv_from(cx, bytes) {
                        Poll::Ready(Ok((n, addr))) => {
                            pinned.tmp_addr = Some(addr);
                            pinned.rd.advance_mut(n);
                            match pinned.codec.decode(&mut pinned.rd) {
                                Ok(None) => (),
                                Ok(Some(e)) => return Poll::Ready(Some(Ok((e, addr)))),
                                Err(e) => {
                                    pinned.is_end = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                        }
                        Poll::Ready(Err(e)) => {
                            pinned.is_end = true;
                            return Poll::Ready(Some(Err(e.into())));
                        }
                        Poll::Pending => {
                            match pinned.codec.decode(&mut pinned.rd) {
                                Ok(None) => {
                                    *pinned.current.lock().unwrap() = Some(cx.waker().clone());
                                    if pinned.shutdown.load(Acquire) {
                                        pinned.is_end = true;
                                        return Poll::Ready(None);
                                    }
                                    // no more Item, ready to do socket.poll_read
                                    pinned.is_ready = true;
                                    return Poll::Pending;
                                }
                                Ok(Some(e)) => {
                                    pinned.is_ready = false;
                                    return Poll::Ready(Some(Ok((e, pinned.tmp_addr.unwrap()))));
                                }
                                Err(e) => {
                                    pinned.is_end = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                        }
                    };
                };
            }
        } else {
            match pinned.codec.decode_eof(&mut pinned.rd) {
                Ok(None) => {
                    *pinned.current.lock().unwrap() = Some(cx.waker().clone());
                    if pinned.shutdown.load(Acquire) {
                        pinned.is_end = true;
                        return Poll::Ready(None);
                    }
                    // no more Item, ready to do socket.poll_read
                    pinned.is_ready = true;
                    Poll::Pending
                }
                Ok(Some(e)) => Poll::Ready(Some(Ok((e, pinned.tmp_addr.unwrap())))),
                Err(e) => {
                    pinned.is_end = true;
                    Poll::Ready(Some(Err(e)))
                }
            }
        }
    }
}

impl<C: Unpin + Decoder> futures::stream::FusedStream for UdpFramed<C> {
    fn is_terminated(&self) -> bool {
        self.is_end
    }
}

impl<T, C: Unpin + Encoder<T> + Decoder> Sink<(T, SocketAddr)> for UdpFramed<C> {
    type Error = <C as Encoder<T>>::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if !self.flushed {
            match self.poll_flush(cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, item: (T, SocketAddr)) -> Result<(), Self::Error> {
        let (frame, out_addr) = item;
        let pinned = Pin::get_mut(self);
        pinned.codec.encode(frame, &mut pinned.wr)?;
        pinned.out_addr = out_addr;
        pinned.flushed = false;
        Ok(())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if self.flushed {
            return Poll::Ready(Ok(()));
        }
        let Self {
            ref mut socket,
            ref mut out_addr,
            ref mut wr,
            ..
        } = *self;
        let n = match socket.poll_send_to(cx, &wr, &out_addr) {
            Poll::Ready(Ok(e)) => e,
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
        };
        let wrote_all = n == self.wr.len();
        self.wr.clear();
        self.flushed = true;
        if wrote_all {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to write entire datagram to socket",
            )
            .into()))
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

const INITIAL_RD_CAPACITY: usize = 64 * 1024;
const INITIAL_WR_CAPACITY: usize = 8 * 1024;

impl<C> UdpFramed<C> {
    /// Create a new `UdpFramed` backed by the given socket and codec.
    pub fn new(socket: UdpSocket, codec: C) -> UdpFramed<C> {
        UdpFramed {
            socket: socket,
            codec: codec,
            out_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
            rd: BytesMut::with_capacity(INITIAL_RD_CAPACITY),
            wr: BytesMut::with_capacity(INITIAL_WR_CAPACITY),
            flushed: true,
            shutdown: Arc::new(AtomicBool::new(false)),
            current: Arc::new(Mutex::new(None)),
            is_ready: true,
            is_end: false,
            tmp_addr: None,
        }
    }

    /// Returns a reference to the underlying I/O stream wrapped by `UdpFramed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_ref(&self) -> &UdpSocket {
        &self.socket
    }

    /// Returns a mutable reference to the underlying I/O stream wrapped by
    /// `UdpFramed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }

    /// Consumes the `Framed`, returning its underlying I/O stream.
    pub fn into_inner(self) -> UdpSocket {
        self.socket
    }

    /// get a Clonable `Shutdown` handler that is thread safe to shutdown this Stream.
    /// It will consume all remaining buffered data and close the stream.
    pub fn get_shutdown(&self) -> Shutdown {
        Shutdown {
            shutdown: self.shutdown.clone(),
            task: self.current.clone(),
        }
    }
}
