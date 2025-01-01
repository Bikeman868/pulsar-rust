use std::sync::RwLock;

use super::MessageLength;

const S_CAPACICY: MessageLength = 64;
const M_CAPACICY: MessageLength = 512;
const L_CAPACICY: MessageLength = 4096;
const XL_CAPACICY: MessageLength = 16384;

pub struct BufferPool {
    s: RwLock<Vec<Vec<u8>>>,
    m: RwLock<Vec<Vec<u8>>>,
    l: RwLock<Vec<Vec<u8>>>,
    xl: RwLock<Vec<Vec<u8>>>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            s: RwLock::new(Vec::new()),
            m: RwLock::new(Vec::new()),
            l: RwLock::new(Vec::new()),
            xl: RwLock::new(Vec::new()),
        }
    }

    pub fn get(self: &Self, size: MessageLength) -> Vec<u8> {
        if size <= S_CAPACICY { BufferPool::get_internal(&self.s, S_CAPACICY, size) }
        else if size <= M_CAPACICY { BufferPool::get_internal(&self.m, M_CAPACICY, size) }
        else if size <= L_CAPACICY { BufferPool::get_internal(&self.l, L_CAPACICY, size) }
        else { BufferPool::get_internal(&self.xl, XL_CAPACICY, size) }
    }

    pub fn reuse(self: &Self, buffer: Vec<u8>) {
        let capacity = buffer.capacity() as MessageLength;
        if capacity <= S_CAPACICY { BufferPool::reuse_internal(&self.s, buffer) }
        else if capacity <= M_CAPACICY { BufferPool::reuse_internal(&self.m, buffer) }
        else if capacity <= L_CAPACICY { BufferPool::reuse_internal(&self.l, buffer) }
        else { BufferPool::reuse_internal(&self.xl, buffer); }
    }

    fn reuse_internal(pool: &RwLock<Vec<Vec<u8>>>, buffer: Vec<u8>) {
        pool.write().unwrap().push(buffer);
    }

    fn get_internal(pool: &RwLock<Vec<Vec<u8>>>, capacity: MessageLength, size: MessageLength) -> Vec<u8> {
        let mut pool = pool.write().unwrap();
        let mut buffer = match pool.pop() {
            Some(buffer) => buffer,
            None => Vec::with_capacity(capacity as usize),
        };
        buffer.resize(size as usize, 0);
        buffer
    }
}
