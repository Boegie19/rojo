use std::{collections::HashMap, path::PathBuf};

#[derive(Debug)]
pub struct ChangeBypass {
    inner: HashMap<PathBuf,()>
}
impl ChangeBypass {
    pub fn new() -> ChangeBypass {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn needs_bypass(&mut self, path: PathBuf) -> bool {
        match self.inner.remove(&path) {
            Some(_) =>  return true,
            None => return false
        }
    }

    pub fn insert(&mut self, path: PathBuf) {
        self.inner.insert(path, ());
    }
}
