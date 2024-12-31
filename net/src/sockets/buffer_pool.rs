use super::MessageLength;

pub struct BufferPool {

}

// TODO: Create pools of vectors with power of 2 capacity then reuse them by setting the vector length
// on get. This will save thrashing the heap with large byte arrays

impl BufferPool {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get(self: &Self, size: MessageLength) -> Vec<u8> {
        let len: usize = size.into();
        let mut vec = Vec::with_capacity(len);
        vec.resize(len, 0);
        vec
    }

    pub fn reuse(self: &Self, buffer: Vec<u8>) {
        drop(buffer);
    }
}
