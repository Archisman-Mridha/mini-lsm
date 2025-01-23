use {super::memtable::MemTable, bytes::Bytes, parking_lot::RwLock, std::sync::Arc};

pub struct StorageEngineState {
  // A memtable usually has a size limit (256 MB in our case), and it will be frozen to an
  // immutable memtable when it reaches the size limit.
  pub(crate) mutableMemtable: Arc<MemTable>,
}

impl StorageEngineState {
  pub fn new() -> Self {
    Self {
      mutableMemtable: Arc::new(MemTable::new(0)),
    }
  }
}

pub struct StorageEngineCore {
  state: Arc<RwLock<Arc<StorageEngineState>>>,
}

impl StorageEngineCore {
  pub fn new() -> Self {
    Self {
      state: Arc::new(RwLock::new(Arc::new(StorageEngineState::new()))),
    }
  }
}

impl StorageEngineCore {
  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    self.state.read().mutableMemtable.get(key)
  }

  pub fn put(&self, key: &[u8], value: &[u8]) {
    self.state.read().mutableMemtable.put(key, value);
  }

  pub fn delete(&self, key: &[u8]) {
    self.state.read().mutableMemtable.delete(key);
  }
}

pub struct StorageEngine {
  core: StorageEngineCore,
}

impl StorageEngine {
  pub fn new() -> Self {
    Self {
      core: StorageEngineCore::new(),
    }
  }
}
