//! Minimal libzmq subscriber and monitor. The library is optional at runtime.
#[cfg(unix)]
mod unix {
    use anyhow::{Context, Result, ensure};
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::mem;
    use std::ptr::NonNull;

    type Ptr = *mut c_void;
    const PAIR: c_int = 0;
    const SUB: c_int = 2;
    const SUBSCRIBE: c_int = 6;
    const RCVMORE: c_int = 13;
    const LINGER: c_int = 17;
    const MAXMSGSIZE: c_int = 22;
    const RCVHWM: c_int = 24;
    const RCVTIMEO: c_int = 27;
    const IPV6: c_int = 42;
    const DONTWAIT: c_int = 1;
    const MONITOR_EVENTS: c_int = 4 | 512 | 4096 | 8192 | 16384 | 32768;

    struct Library {
        handle: NonNull<c_void>,
        ctx_new: unsafe extern "C" fn() -> Ptr,
        ctx_term: unsafe extern "C" fn(Ptr) -> c_int,
        socket: unsafe extern "C" fn(Ptr, c_int) -> Ptr,
        close: unsafe extern "C" fn(Ptr) -> c_int,
        connect: unsafe extern "C" fn(Ptr, *const c_char) -> c_int,
        socket_monitor: unsafe extern "C" fn(Ptr, *const c_char, c_int) -> c_int,
        setsockopt: unsafe extern "C" fn(Ptr, c_int, *const c_void, usize) -> c_int,
        getsockopt: unsafe extern "C" fn(Ptr, c_int, Ptr, *mut usize) -> c_int,
        recv: unsafe extern "C" fn(Ptr, Ptr, usize, c_int) -> c_int,
    }

    impl Library {
        fn open() -> Result<Self> {
            let handle = [c"libzmq.so.5", c"libzmq.5.dylib", c"libzmq.so"]
                .into_iter()
                .find_map(|name| {
                    NonNull::new(unsafe {
                        libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL)
                    })
                })
                .context("libzmq runtime unavailable")?;
            // Each signature corresponds to the named libzmq C API symbol.
            let result = (|| {
                macro_rules! resolve {
                    ($name:ident, $signature:ty) => {{
                        let name = CString::new(concat!("zmq_", stringify!($name)))?;
                        let address = unsafe { libc::dlsym(handle.as_ptr(), name.as_ptr()) };
                        ensure!(
                            !address.is_null(),
                            "missing libzmq symbol {}",
                            name.to_string_lossy()
                        );
                        unsafe { mem::transmute::<Ptr, $signature>(address) }
                    }};
                }
                Ok(Self {
                    handle,
                    ctx_new: resolve!(ctx_new, unsafe extern "C" fn() -> Ptr),
                    ctx_term: resolve!(ctx_term, unsafe extern "C" fn(Ptr) -> c_int),
                    socket: resolve!(socket, unsafe extern "C" fn(Ptr, c_int) -> Ptr),
                    close: resolve!(close, unsafe extern "C" fn(Ptr) -> c_int),
                    connect: resolve!(connect, unsafe extern "C" fn(Ptr, *const c_char) -> c_int),
                    socket_monitor: resolve!(
                        socket_monitor,
                        unsafe extern "C" fn(Ptr, *const c_char, c_int) -> c_int
                    ),
                    setsockopt: resolve!(
                        setsockopt,
                        unsafe extern "C" fn(Ptr, c_int, *const c_void, usize) -> c_int
                    ),
                    getsockopt: resolve!(
                        getsockopt,
                        unsafe extern "C" fn(Ptr, c_int, Ptr, *mut usize) -> c_int
                    ),
                    recv: resolve!(recv, unsafe extern "C" fn(Ptr, Ptr, usize, c_int) -> c_int),
                })
            })();
            if result.is_err() {
                unsafe { libc::dlclose(handle.as_ptr()) };
            }
            result
        }
    }

    impl Drop for Library {
        fn drop(&mut self) {
            unsafe { libc::dlclose(self.handle.as_ptr()) };
        }
    }

    pub(super) struct Connection {
        lib: Library,
        ctx: Ptr,
        sub: Ptr,
        monitor: Ptr,
        buffer: Vec<u8>,
    }

    impl Connection {
        pub(super) fn open(endpoint: &str, topics: &[super::super::Topic]) -> Result<Self> {
            let lib = Library::open()?;
            let ctx = unsafe { (lib.ctx_new)() };
            ensure!(!ctx.is_null(), "could not create ZMQ context");
            let mut connection = Self {
                lib,
                ctx,
                sub: std::ptr::null_mut(),
                monitor: std::ptr::null_mut(),
                buffer: vec![0; super::super::wire::MAX_BODY],
            };
            let ipv6 = endpoint.starts_with("tcp://[");
            let endpoint = CString::new(endpoint)?;
            let monitor_endpoint = c"inproc://carbide-monitor";
            connection.sub = unsafe { (connection.lib.socket)(ctx, SUB) };
            ensure!(!connection.sub.is_null(), "could not create ZMQ subscriber");
            connection.set(connection.sub, LINGER, &0i32.to_ne_bytes())?;
            connection.set(connection.sub, RCVHWM, &32i32.to_ne_bytes())?;
            connection.set(connection.sub, RCVTIMEO, &40i32.to_ne_bytes())?;
            if ipv6 {
                connection.set(connection.sub, IPV6, &1i32.to_ne_bytes())?;
            }
            connection.set(
                connection.sub,
                MAXMSGSIZE,
                &(super::super::wire::MAX_BODY as i64).to_ne_bytes(),
            )?;
            for topic in topics {
                connection.set(connection.sub, SUBSCRIBE, topic.name().as_bytes())?;
            }
            ensure!(
                unsafe {
                    (connection.lib.socket_monitor)(
                        connection.sub,
                        monitor_endpoint.as_ptr(),
                        MONITOR_EVENTS,
                    )
                } == 0,
                "could not start ZMQ socket monitor"
            );
            connection.monitor = unsafe { (connection.lib.socket)(ctx, PAIR) };
            ensure!(
                !connection.monitor.is_null(),
                "could not open ZMQ socket monitor"
            );
            connection.set(connection.monitor, LINGER, &0i32.to_ne_bytes())?;
            ensure!(
                unsafe { (connection.lib.connect)(connection.monitor, monitor_endpoint.as_ptr()) }
                    == 0,
                "could not connect ZMQ socket monitor"
            );
            ensure!(
                unsafe { (connection.lib.connect)(connection.sub, endpoint.as_ptr()) } == 0,
                "could not connect to ZMQ endpoint"
            );
            Ok(connection)
        }

        fn set(&self, socket: Ptr, option: c_int, value: &[u8]) -> Result<()> {
            ensure!(
                unsafe {
                    (self.lib.setsockopt)(socket, option, value.as_ptr().cast(), value.len())
                } == 0,
                "could not configure ZMQ socket"
            );
            Ok(())
        }

        fn frame(&mut self, socket: Ptr, flags: c_int) -> Option<(&[u8], bool)> {
            let size = unsafe {
                (self.lib.recv)(
                    socket,
                    self.buffer.as_mut_ptr().cast(),
                    self.buffer.len(),
                    flags,
                )
            };
            if size < 0 {
                return None;
            }
            let mut more = 0i32;
            let mut length = mem::size_of_val(&more);
            let result = unsafe {
                (self.lib.getsockopt)(
                    socket,
                    RCVMORE,
                    std::ptr::from_mut(&mut more).cast(),
                    &mut length,
                )
            };
            (result == 0).then(|| {
                (
                    &self.buffer[..(size as usize).min(self.buffer.len())],
                    more != 0,
                )
            })
        }

        fn drain(&mut self, socket: Ptr, mut more: bool) {
            while more {
                more = self.frame(socket, DONTWAIT).is_some_and(|(_, more)| more);
            }
        }

        pub(super) fn monitor(&mut self) -> Option<u16> {
            let socket = self.monitor;
            let (bytes, more) = self.frame(socket, DONTWAIT)?;
            let code = bytes
                .get(..2)
                .map(|code| u16::from_ne_bytes([code[0], code[1]]));
            self.drain(socket, more);
            code
        }

        pub(super) fn receive(&mut self) -> Option<super::Message> {
            let socket = self.sub;
            let (topic, more) = self.frame(socket, 0)?;
            let topic = super::super::Topic::from_frame(topic);
            if !more {
                return Some(super::Message::Malformed);
            }
            let (payload, more) = self.frame(socket, DONTWAIT)?;
            let payload = topic.map(|_| payload.to_vec());
            if !more {
                return Some(super::Message::Malformed);
            }
            let (sequence, more) = self.frame(socket, DONTWAIT)?;
            let sequence = <[u8; 4]>::try_from(sequence).ok().map(u32::from_le_bytes);
            self.drain(socket, more);
            match (topic, payload, sequence, more) {
                (Some(topic), Some(payload), Some(sequence), false) => {
                    Some(super::Message::Data(topic, payload, sequence))
                }
                _ => Some(super::Message::Malformed),
            }
        }
    }

    impl Drop for Connection {
        fn drop(&mut self) {
            for socket in [self.monitor, self.sub] {
                if !socket.is_null() {
                    unsafe { (self.lib.close)(socket) };
                }
            }
            unsafe { (self.lib.ctx_term)(self.ctx) };
        }
    }
}

use super::Topic;
use std::sync::atomic::AtomicBool;

enum Message {
    Data(Topic, Vec<u8>, u32),
    Malformed,
}

pub(super) enum Event {
    Connected,
    Unreachable,
    Data(Topic, Vec<u8>, u32),
    Malformed,
    Error(String),
}

#[cfg(unix)]
pub(super) fn subscribe(
    endpoint: &str,
    topics: &[Topic],
    stop: &AtomicBool,
    mut publish: impl FnMut(Event),
) {
    use std::sync::atomic::Ordering;
    let mut connection = match unix::Connection::open(endpoint, topics) {
        Ok(connection) => connection,
        Err(error) => {
            publish(Event::Error(error.to_string()));
            return;
        }
    };
    while !stop.load(Ordering::Relaxed) {
        while let Some(code) = connection.monitor() {
            match code {
                4096 => publish(Event::Connected),
                4 | 512 | 8192 | 16384 | 32768 => publish(Event::Unreachable),
                _ => {}
            }
        }
        match connection.receive() {
            Some(Message::Data(topic, bytes, sequence)) => {
                publish(Event::Data(topic, bytes, sequence))
            }
            Some(Message::Malformed) => publish(Event::Malformed),
            None => {}
        }
    }
}

#[cfg(not(unix))]
pub(super) fn subscribe(_: &str, _: &[Topic], _: &AtomicBool, mut publish: impl FnMut(Event)) {
    publish(Event::Error(
        "ZMQ transport is unavailable on this platform".into(),
    ));
}
