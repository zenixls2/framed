#![feature(test)]
pub mod shutdown;
pub mod tcp;
pub mod udp;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
