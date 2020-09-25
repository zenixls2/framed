use crate::shutdown::Shutdown;
use bytes::buf::Buf;
use bytes::BytesMut;
use core::pin::Pin;
use core::task::Waker;
use futures::prelude::*;
use futures::task::{Context, Poll};
use std::io;
use std::marker::Unpin;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder};

pub struct TcpFramed<C> {
    socket: TcpStream,
    codec: C,
    rd: BytesMut,
    wr: BytesMut,
    flushed: bool,
    shutdown: Arc<AtomicBool>,
    // task will be saved here before NotReady
    current: Arc<Mutex<Option<Waker>>>,
    // flag to check whether to do poll_read on socket
    is_ready: bool,
    is_end: bool,
}

impl<C: Unpin + Decoder> Stream for TcpFramed<C> {
    type Item = Result<C::Item, C::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = Pin::get_mut(self);
        if pinned.is_ready {
            loop {
                // make sure rd is empty or no more msg could be exported
                match pinned.codec.decode(&mut pinned.rd) {
                    Ok(None) => {}
                    Ok(Some(e)) => return Poll::Ready(Some(Ok(e))),
                    Err(e) => {
                        pinned.is_end = true;
                        return Poll::Ready(Some(Err(From::from(e))));
                    }
                };
                pinned.rd.reserve(INITIAL_RD_CAPACITY);
                match Pin::new(&mut pinned.socket).poll_read_buf(cx, &mut pinned.rd) {
                    Poll::Ready(Ok(t)) => {
                        if t == 0 {
                            if pinned.rd.is_empty() {
                                pinned.is_end = true;
                                return Poll::Ready(None);
                            } else {
                                pinned.is_end = true;
                                return Poll::Ready(Some(Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "bytes remaining on stream",
                                )
                                .into())));
                            }
                        } else {
                            match pinned.codec.decode(&mut pinned.rd) {
                                Ok(None) => (),
                                Ok(Some(e)) => return Poll::Ready(Some(Ok(e))),
                                Err(e) => {
                                    pinned.is_end = true;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                        }
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
                                return Poll::Ready(Some(Ok(e)));
                            }
                            Err(e) => {
                                pinned.is_end = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        };
                    }
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
                Ok(Some(e)) => Poll::Ready(Some(Ok(e))),
                Err(e) => {
                    pinned.is_end = true;
                    Poll::Ready(Some(Err(e)))
                }
            }
        }
    }
}

impl<C: Unpin + Decoder> futures::stream::FusedStream for TcpFramed<C> {
    fn is_terminated(&self) -> bool {
        self.is_end
    }
}

impl<T, C: Unpin + Encoder<T> + Decoder> Sink<T> for TcpFramed<C> {
    type Error = <C as Encoder<T>>::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if self.wr.len() >= BACKPRESSURE_BOUNDARY {
            match self.as_mut().poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => (),
            };
            if self.wr.len() >= BACKPRESSURE_BOUNDARY {
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let pinned = Pin::get_mut(self);
        pinned.codec.encode(item, &mut pinned.wr)?;
        pinned.flushed = false;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let pinned = Pin::get_mut(self);
        if pinned.flushed {
            return Poll::Ready(Ok(()));
        }
        while !pinned.wr.is_empty() {
            let n = match Pin::new(&mut pinned.socket).poll_write(cx, &pinned.wr) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                Poll::Pending => return Poll::Pending,
            };
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write frame to transport",
                )
                .into()));
            }
            pinned.wr.advance(n);
        }
        match Pin::new(&mut pinned.socket).poll_flush(cx) {
            Poll::Ready(Ok(())) => (),
            Poll::Ready(Err(e)) => return Poll::Ready(Err(From::from(e))),
            Poll::Pending => return Poll::Pending,
        };
        pinned.flushed = true;
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        };

        match Pin::new(&mut self.socket).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(Ok(()))
    }
}

const INITIAL_RD_CAPACITY: usize = 64 * 1024;
const INITIAL_WR_CAPACITY: usize = 8 * 1024;
const BACKPRESSURE_BOUNDARY: usize = INITIAL_WR_CAPACITY;

impl<C> TcpFramed<C> {
    /// Create a new `TcpFramed` backed by the given socket and codec.
    pub fn new(socket: TcpStream, codec: C) -> TcpFramed<C> {
        TcpFramed {
            socket: socket,
            codec: codec,
            rd: BytesMut::with_capacity(INITIAL_RD_CAPACITY),
            wr: BytesMut::with_capacity(INITIAL_WR_CAPACITY),
            flushed: true,
            shutdown: Arc::new(AtomicBool::new(false)),
            current: Arc::new(Mutex::new(None)),
            is_ready: true,
            is_end: false,
        }
    }

    /// Returns a refernce to the underlying I/O stream wrapped by `TcpFramed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_ref(&self) -> &TcpStream {
        &self.socket
    }

    /// Returns a mutable refernce to the underlying I/O stream wrapped by
    /// `TcpFramed`.
    ///
    /// # Note
    ///
    /// Care should be taken to not tamper with the underlying stream of data
    /// coming in as it may corrupt the stream of frames otherwise being worked
    /// with.
    pub fn get_mut(&mut self) -> &mut TcpStream {
        &mut self.socket
    }

    /// Consumes the `TcpFramed`, returning its underlying I/O stream.
    pub fn into_inner(self) -> TcpStream {
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
