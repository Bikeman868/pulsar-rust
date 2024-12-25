use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub struct DualKeyHashMap<K1: Eq + Hash, K2: Eq + Hash, V: Default> {
    values: Vec<V>,
    removed: VecDeque<usize>,
    map1: HashMap<K1, usize>,
    map2: HashMap<K2, usize>,
}

impl<K1: Eq + Hash, K2: Eq + Hash, V: Default> DualKeyHashMap<K1, K2, V> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            removed: VecDeque::new(),
            map1: HashMap::new(),
            map2: HashMap::new(),
        }
    }

    pub fn insert(self: &mut Self, key1: K1, key2: K2, value: V) {
        let index = match self.removed.pop_front() {
            Some(index) => {
                self.values[index] = value;
                index
            }
            None => {
                self.values.push(value);
                self.values.len() - 1
            }
        };
        self.map1.insert(key1, index);
        self.map2.insert(key2, index);
    }

    pub fn get_using_key1<'a>(self: &'a Self, key: &K1) -> Option<&'a V> {
        match self.map1.get(key) {
            Some(index) => Some(&self.values[*index]),
            None => None,
        }
    }

    pub fn get_mut_using_key1<'a>(self: &'a mut Self, key: &K1) -> Option<&'a mut V> {
        match self.map1.get(key) {
            Some(index) => Some(self.values.get_mut(*index)?),
            None => None,
        }
    }

    pub fn get_using_key2<'a>(self: &'a Self, key: &K2) -> Option<&'a V> {
        match self.map2.get(key) {
            Some(index) => Some(&self.values[*index]),
            None => None,
        }
    }

    pub fn get_mut_using_key2<'a>(self: &'a mut Self, key: &K2) -> Option<&'a mut V> {
        match self.map2.get(key) {
            Some(index) => Some(self.values.get_mut(*index)?),
            None => None,
        }
    }

    pub fn remove(self: &mut Self, key1: &K1, key2: &K2) -> bool {
        let hash1 = &mut self.map1;
        let hash2 = &mut self.map2;

        let index1 = match hash1.get(key1) {
            Some(index) => *index,
            None => {
                return false;
            }
        };

        let index2 = match hash2.get(key2) {
            Some(index) => *index,
            None => {
                return false;
            }
        };

        if index1 != index2 {
            panic!("Attempt to remove from DualKeyHashMap with non-matching keys")
        }

        hash1.remove(key1);
        hash2.remove(key2);

        self.values[index1] = V::default();
        self.removed.push_back(index1);

        return true;
    }

    pub fn remove_using_key1<F>(self: &mut Self, key1: &K1, f: F) -> bool
    where
        F: FnOnce(&V) -> &K2,
    {
        let index = match self.map1.get(key1) {
            Some(index) => *index,
            None => {
                return false;
            }
        };

        let key2 = f(&self.values[index]);

        self.map1.remove(key1);
        self.map2.remove(&key2);

        self.values[index] = V::default();
        self.removed.push_back(index);

        return true;
    }

    pub fn remove_using_key2<F>(self: &mut Self, key2: &K2, f: F) -> bool
    where
        F: FnOnce(&V) -> &K1,
    {
        let index = match self.map2.get(key2) {
            Some(index) => *index,
            None => {
                return false;
            }
        };

        let key1 = f(&self.values[index]);

        self.map1.remove(&key1);
        self.map2.remove(key2);

        self.values[index] = V::default();
        self.removed.push_back(index);

        return true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NameOfNumber {
        key1: &'static str,
        key2: u16,
        name: &'static str,
    }

    impl Default for NameOfNumber {
        fn default() -> Self {
            Self {
                key1: "0",
                key2: 0,
                name: "Zero",
            }
        }
    }

    #[test]
    pub fn should_find_using_either_key() {
        let mut data: DualKeyHashMap<&str, u16, NameOfNumber> = DualKeyHashMap::new();

        data.insert(
            "1",
            1,
            NameOfNumber {
                key1: "1",
                key2: 1,
                name: "One",
            },
        );
        data.insert(
            "2",
            2,
            NameOfNumber {
                key1: "2",
                key2: 2,
                name: "Two",
            },
        );
        data.insert(
            "3",
            3,
            NameOfNumber {
                key1: "3",
                key2: 3,
                name: "Three",
            },
        );

        assert_eq!("One", data.get_using_key1(&"1").unwrap().name);
        assert_eq!("Two", data.get_using_key1(&"2").unwrap().name);
        assert_eq!("Three", data.get_using_key1(&"3").unwrap().name);

        assert_eq!("One", data.get_using_key2(&1).unwrap().name);
        assert_eq!("Two", data.get_using_key2(&2).unwrap().name);
        assert_eq!("Three", data.get_using_key2(&3).unwrap().name);
    }

    #[test]
    pub fn should_remove_using_both_keys() {
        let mut data: DualKeyHashMap<&str, u16, NameOfNumber> = DualKeyHashMap::new();

        data.insert(
            "1",
            1,
            NameOfNumber {
                key1: "1",
                key2: 1,
                name: "One",
            },
        );
        data.insert(
            "2",
            2,
            NameOfNumber {
                key1: "2",
                key2: 2,
                name: "Two",
            },
        );
        data.insert(
            "3",
            3,
            NameOfNumber {
                key1: "3",
                key2: 3,
                name: "Three",
            },
        );
        data.remove(&"2", &2);
        data.insert(
            "4",
            4,
            NameOfNumber {
                key1: "4",
                key2: 4,
                name: "Four",
            },
        );

        assert_eq!("One", data.get_using_key1(&"1").unwrap().name);
        assert_eq!("Three", data.get_using_key1(&"3").unwrap().name);
        assert_eq!("Four", data.get_using_key1(&"4").unwrap().name);
        assert!(data.get_using_key1(&"2").is_none());
        assert!(data.get_using_key2(&2).is_none());
    }

    #[test]
    pub fn should_remove_using_key1() {
        let mut data: DualKeyHashMap<&str, u16, NameOfNumber> = DualKeyHashMap::new();

        data.insert(
            "1",
            1,
            NameOfNumber {
                key1: "1",
                key2: 1,
                name: "One",
            },
        );
        data.insert(
            "2",
            2,
            NameOfNumber {
                key1: "2",
                key2: 2,
                name: "Two",
            },
        );
        data.insert(
            "3",
            3,
            NameOfNumber {
                key1: "3",
                key2: 3,
                name: "Three",
            },
        );

        assert!(data.remove_using_key1(&"2", |v| &v.key2));
        assert!(data.get_using_key1(&"2").is_none());
        assert!(data.get_using_key2(&2).is_none());
    }

    #[test]
    pub fn should_remove_using_key2() {
        let mut data: DualKeyHashMap<&str, u16, NameOfNumber> = DualKeyHashMap::new();

        data.insert(
            "1",
            1,
            NameOfNumber {
                key1: "1",
                key2: 1,
                name: "One",
            },
        );
        data.insert(
            "2",
            2,
            NameOfNumber {
                key1: "2",
                key2: 2,
                name: "Two",
            },
        );
        data.insert(
            "3",
            3,
            NameOfNumber {
                key1: "3",
                key2: 3,
                name: "Three",
            },
        );

        assert!(data.remove_using_key2(&2, |v| &v.key1));
        assert!(data.get_using_key1(&"2").is_none());
        assert!(data.get_using_key2(&2).is_none());
    }
}
