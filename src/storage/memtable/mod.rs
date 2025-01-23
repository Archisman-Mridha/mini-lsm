use {bytes::Bytes, crossbeam_skiplist::SkipMap, std::sync::Arc};

pub struct MemTable {
  id: usize,
  skipMap: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
  pub fn new(id: usize) -> Self {
    Self {
      id,
      skipMap: Arc::new(SkipMap::new()),
    }
  }
}

impl MemTable {
  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    let entry = self.skipMap.get(&Bytes::copy_from_slice(key))?;
    Some(entry.value().clone())
  }

  pub fn put(&self, key: &[u8], value: &[u8]) {
    self
      .skipMap
      .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
  }

  // Inserts an empty byte slice for the given key, in the mutableMemtable.
  // This entry is called a delete tombstone.
  pub fn delete(&self, key: &[u8]) {
    self.put(key, &[]);
  }
}
