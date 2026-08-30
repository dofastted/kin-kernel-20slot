use std::collections::{HashMap, VecDeque};

use super::slot::{Slot, SlotPhase};
use crate::error::KernelError;

pub struct SlotScheduler {
    ready: VecDeque<String>,
    sticky: HashMap<String, String>,
}

impl SlotScheduler {
    pub fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            sticky: HashMap::new(),
        }
    }

    pub fn enqueue_ready(&mut self, slot_id: String) -> bool {
        if self.ready.iter().any(|id| id == &slot_id) {
            return false;
        }
        self.ready.push_back(slot_id);
        true
    }

    pub fn forget(&mut self, slot_id: &str) {
        self.ready.retain(|id| id != slot_id);
        self.sticky.retain(|_, bound| bound != slot_id);
    }

    pub fn bind_sticky(&mut self, session_key: String, slot_id: String) {
        self.sticky.insert(session_key, slot_id);
    }

    pub fn pick(
        &mut self,
        slots: &mut [Slot],
        tenant_id: &str,
        session_id: &str,
    ) -> Result<String, KernelError> {
        let key = format!("{tenant_id}\u{1f}{session_id}");
        if let Some(id) = self.sticky.get(&key).cloned()
            && let Some(slot) = slots.iter_mut().find(|slot| slot.id == id)
            && slot.phase == SlotPhase::ReadyBlocked
            && slot.tenant_id.as_deref().unwrap_or(tenant_id) == tenant_id
        {
            self.ready.retain(|item| item != &id);
            return Ok(id);
        }

        while let Some(id) = self.ready.pop_front() {
            let Some(slot) = slots.iter().find(|slot| slot.id == id) else {
                continue;
            };
            if slot.phase != SlotPhase::ReadyBlocked {
                continue;
            }
            if let Some(tenant) = slot.tenant_id.as_deref()
                && tenant != tenant_id
            {
                self.ready.push_back(id.clone());
                if self.ready.front().map(String::as_str) == Some(id.as_str()) {
                    break;
                }
                continue;
            }
            self.bind_sticky(key, id.clone());
            return Ok(id);
        }
        Err(KernelError::NoCapacity)
    }
}

impl Default for SlotScheduler {
    fn default() -> Self {
        Self::new()
    }
}
