use std::collections::HashSet;
use std::hash::Hash;

/// This is a placeholder for a faster implementation later, where we can take advantage of
/// knowing that there are very large ranges of consecutive message ids that are acked.
/// 
/// The idea is to keep a boolean flag for each message IDs to say if it was acked. Rather than
/// storing the message ids in a HashSet, it's much more efficient to have a linked list of the
/// ranges of IDs that have been acked. For example 1645-8764 are acked and 8767-9763 are acked
/// implying that 8765-8766 are not acked. If we can binary search the list of ranges, I think this
/// should be compact and efficient.
pub struct BigSet<T: Eq + Ord + Hash + Copy> {
    hash_set: HashSet<T>,
}

impl<T: Eq + Ord + Hash + Copy> BigSet<T> {
    pub fn new() -> Self {
        Self{
            hash_set: HashSet::new(),
        }
    }

    pub fn add(self: &mut Self, value: T) {
        self.hash_set.insert(value);
    }

    pub fn remove(self: &mut Self, value: T) {
        self.hash_set.remove(&value);
    }

    pub fn contains(self: &Self, value: T) -> bool {
        self.hash_set.contains(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut set: BigSet<u32> = BigSet::new();
        for i in 45..89 { set.add(i) }
        for i in 150..190 { set.add(i) }

        for i in 0..45 { assert!(!set.contains(i)) }
        for i in 45..89 { assert!(set.contains(i)) }
        for i in 89..150 { assert!(!set.contains(i)) }
        for i in 150..190 { assert!(set.contains(i)) }
        for i in 190..250 { assert!(!set.contains(i)) }

        set.add(100);
        assert!(set.contains(100));
        assert!(!set.contains(99));
        assert!(!set.contains(101));

        set.remove(45);
        set.remove(50);
        set.remove(88);

        assert!(!set.contains(50));
        assert!(set.contains(100));
        for i in 0..46 { assert!(!set.contains(i)) }
        for i in 46..50 { assert!(set.contains(i)) }
        for i in 51..88 { assert!(set.contains(i)) }
        for i in 88..100 { assert!(!set.contains(i)) }
        for i in 101..150 { assert!(!set.contains(i)) }
    }
}
