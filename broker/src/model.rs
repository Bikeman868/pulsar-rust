/*
Defines the shared internal state of the application. Some of this state is
persisted to the database, and some is just updated in memory.
To recover the proper state after a restart, the applications persists an event
log that can be replayed at startup.
*/

use std::{
    collections::HashMap, 
    hash::Hash, 
    marker::PhantomData, 
    sync::{
        Arc, RwLock
    }
};

pub mod cluster;
pub mod ledger;
pub mod messages;
pub mod node;
pub mod partition;
pub mod request_mapping;
pub mod response_mapping;
pub mod subscription;
pub mod topic;

#[cfg_attr(debug_assertions, derive(Debug))]
pub enum RefreshStatus {
    /// Entity was successfully refreshed from the database
    Updated,

    /// Entity contains stale data because the refresh failed
    Stale,

    /// This entity was not found in the database
    Deleted,
}

/// All model entities should implement the Entity trait so that we can add them to a HashMap
pub trait Entity<K> where K: Eq + Hash + Clone { 
    fn key(self: &Self) -> K;
}

/// Represents a thread-safe sharable reference to a model entity (such as Ledger, Topic etc)
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct EntityRef<K, E> where K: Eq + Hash + Clone, E: Entity<K>{ 
    entity: Arc<E>,
    phantom_data: PhantomData<K>,
}

// Note that we don't want to be able to get a mutable reference to an entity. Entities should use
// internal mutability with RwLock so that a mutable reference is not needed. You can't get a mutable
// reference becausse these entities are accessed randomly by multiple threads.
impl<K, E> EntityRef<K, E> where K: Eq + Hash + Clone, E: Entity<K> {
    pub fn new(entity: E) -> Self { Self { entity: Arc::new(entity), phantom_data: PhantomData }}
    pub fn copy(entity_ref: EntityRef<K, E>) -> Self { Self { entity: entity_ref.entity.clone(), phantom_data: PhantomData} }
    pub fn entity(self: &Self) -> &Arc<E> { &self.entity }
    pub fn key(self: &Self) -> K { self.entity.key() }
    // pub fn get_mut(self: &mut Self) -> Option<&mut E> { Arc::get_mut(&mut self.entity) }
}

/// Cloning the entity reference gives you a counted reference to the same entity
/// Dropping this data decrements to the reference count on the underlying data
impl<K, E> Clone for EntityRef<K, E> where K: Eq + Hash + Clone, E: Entity<K> {
    fn clone(self: &Self) -> Self {
        Self { entity: self.entity.clone(), phantom_data: PhantomData }
    }
}

impl<K, E> std::ops::Deref for EntityRef<K, E> where K: Eq + Hash + Clone, E: Entity<K> {
    type Target = E;
    fn deref(&self) -> &Self::Target { &self.entity }
}

/// Represents a thread-safe sharable reference to a list of model entities (for example a list of Ledgers)
#[cfg_attr(debug_assertions, derive(Debug))]
pub struct EntityList<K, E> where K: Eq + Hash + Clone, E: Entity<K> {
    entities: RwLock<HashMap<K, EntityRef<K, E>>>,
}

impl<K, E> EntityList<K, E> where K: Eq + Hash + Clone, E: Entity<K> {
    pub fn new() -> Self { 
        Self { entities: RwLock::new(HashMap::new()) }
    }
    
    /// Builds an EntityList from an Iterator of EntityRef
    pub fn from_iter_ref(entities: impl Iterator<Item = EntityRef<K, E>>) -> Self {
        let entities = entities.map(|entity_ref| (entity_ref.key(), entity_ref.clone())).collect();
        Self { entities: RwLock::new(entities) }
    }
    
    /// Builds an EntityList from an Iterator of Entity
    pub fn from_iter(entities: impl Iterator<Item = E>) -> Self {
        let entities = entities.map(|entity| (entity.key(), EntityRef::new(entity))).collect();
        Self { entities: RwLock::new(entities) }
    }

    pub fn insert(self: &Self, entity: E) -> Option<EntityRef<K, E>> {
        let mut entities = self.entities.write().unwrap();
        entities.insert(entity.key(), EntityRef::new(entity))
    }
    
    pub fn insert_ref(self: &Self, entity_ref: EntityRef<K, E>) -> Option<EntityRef<K, E>> {
        let mut entities = self.entities.write().unwrap();
        entities.insert(entity_ref.key(), entity_ref)
    }
    
    pub fn remove(self: &Self, key: &K) -> Option<EntityRef<K, E>> {
        let mut entities = self.entities.write().unwrap();
        entities.remove(key)
    }
    
    /// Retrieves an entity from the list by its key, or None if not in the list
    pub fn get(self: &Self, k: &K) -> Option<EntityRef<K, E>> {
        Some(self.entities.read().unwrap().get(k)?.clone())
    }

    /// Enumerates all of the entities in this list finding the first one that matches the
    /// supplied predicate
    pub fn find(self: &Self, predicate: impl Fn(&EntityRef<K, E>) -> bool) -> Option<EntityRef<K, E>> {
        match self.entities.read().unwrap().iter().find(|&item| predicate(item.1)) {
            Some((_k, entity_ref)) => Some(entity_ref.clone()),
            None => None,
        }
    }

    /// Constructs and returns a vector containing a snapshot of the current entity key.
    /// Note that enumerating the keys and looking up the values may result in
    /// the key not being found because other threads may have removed them
    /// from the underlyng collection
    pub fn keys(self: &Self) -> Vec<K> {
        self.entities.read().unwrap().keys().map(|k|k.clone()).collect()
    }

    /// Constructs and returns a vector containing clones of the entities in the list. These clones 
    /// reference the original entities through an Arc
    pub fn values(self: &Self) -> Vec<EntityRef<K, E>> {
        self.entities.read().unwrap().values().map(|entity_ref|entity_ref.clone()).collect()
    }
}

#[cfg(test)]
mod tests{
    use super::*;

    // The mutable state must be in a separate private struct so that we can protect access
    // through a RwLock
    struct TestEntityState {
        name: String,
    }

    // This is an example of how the model entities are defined. The entity is
    // thread safe but not sharable. To share the entity accross multiple threads
    // it needs to be wrapped in am EntityRef
    pub struct TestEntity {
        id: usize,
        state: RwLock<TestEntityState>,
    }

    impl TestEntity {
        pub fn new(id: usize, name: String) -> Self {
            Self { id, state: RwLock::new(TestEntityState { name }) }
        }

        pub fn id(self: &Self) -> usize{ self.id }
        pub fn name(self: &Self) -> String { self.state.read().unwrap().name.clone() }

        pub fn set_name(self: &Self, name: String) {
            self.state.write().unwrap().name = name;
        }
    }

    impl Entity<usize> for TestEntity { 
        fn key(self: &Self) -> usize { self.id }
    }

    pub struct TestEntityList {
        id: usize,
        test_entities: EntityList<usize, TestEntity>,
    }

    impl TestEntityList {
        pub fn new(id: usize) -> Self {
            Self { id, test_entities: EntityList::new() }
        }

        pub fn test_entities(self: &Self) -> &EntityList<usize, TestEntity> { &self.test_entities }
    }

    impl Entity<usize> for TestEntityList { 
        fn key(self: &Self) -> usize { self.id }
    }

    #[test]
    pub fn thread_safe_sharable_entities() {
        let entity_ref = EntityRef::new(TestEntity::new(1,  String::from("Tester")));

        // Clones of EntityRef refer to the same entity
        let cloned = entity_ref.clone();

        // This might print "Tester" or "Updated" depending on the timing
        let thread = std::thread::spawn(move|| println!("{}", cloned.name()));

        // Internally the entity uses a RwLock to prevent updates during reads
        entity_ref.set_name(String::from("Updated"));
        assert_eq!("Updated", entity_ref.name());

        thread.join().unwrap();
    }

    #[test]
    pub fn thread_safe_sharable_lists() {
        let mut list = Vec::new();
        list.push(TestEntity::new(1, String::from("Test 1")));
        list.push(TestEntity::new(2, String::from("Test 2")));
        list.push(TestEntity::new(3, String::from("Test 3")));

        // EntityList needs to be wrapped in Arc to allow thread sharing
        let entity_list = Arc::new(EntityList::from_iter(list.into_iter()));
 
        // Arc must be cloned to move into a thread
        let clone = entity_list.clone();
        std::thread::spawn(move|| println!("{}", clone.get(&2).unwrap().name()));

        // Arc must be cloned to move into a thread
        let clone = entity_list.clone();
        std::thread::spawn(move|| clone.remove(&3));

        // Because EntityRef is Deref, the syntax is very natural
        for key in entity_list.keys() {
            if let Some(entity) = entity_list.get(&key) {
                println!("{}", entity.name());
            }
        }
        
        if let Some(test1) = entity_list.values().iter().find(|e|e.name() == "Test 1") {
            assert_eq!(1, test1.id());
        };
    }

    #[test]
    pub fn entities_containing_lists() {
        let entity = TestEntityList::new(1);

        entity.test_entities().insert(TestEntity::new(1, String::from("Test 1")));
        entity.test_entities().insert(TestEntity::new(2, String::from("Test 2")));
        entity.test_entities().insert(TestEntity::new(3, String::from("Test 3")));
        
        entity.test_entities().values().iter().for_each(|e| println!("{}", e.name()));

        match entity.test_entities().find(|e|e.name() == "Test 2") {
            Some(entity_ref) => assert_eq!(2, entity_ref.id()),
            None => panic!(),
        }
    }
}
