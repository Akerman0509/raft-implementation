"""
State Machine for RAFT
Key-value store với persistent storage
"""

import json
import logging
from typing import Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class StateMachine:
    """
    Key-Value Store State Machine
    
    Supports:
    - SET key value
    - GET key
    - DELETE key
    - Persistent storage
    """
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.store_file = data_dir / 'kv_store.json'
        self.store: Dict[str, str] = {}
        
        # Load existing store
        self._load_store()
        
        logger.info(f"StateMachine initialized with {len(self.store)} keys")
    
    def _load_store(self):
        """Load key-value store from persistent storage"""
        if self.store_file.exists():
            try:
                with open(self.store_file, 'r') as f:
                    self.store = json.load(f)
                logger.info(f"Loaded {len(self.store)} keys from {self.store_file}")
            except Exception as e:
                logger.error(f"Failed to load store: {e}")
                self.store = {}
        else:
            self.store = {}
            logger.info("No existing store file, starting with empty store")
    
    def _save_store(self):
        """Save key-value store to persistent storage"""
        try:
            with open(self.store_file, 'w') as f:
                json.dump(self.store, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save store: {e}")
    
    def apply(self, command: str) -> str:
        """
        Apply a command to state machine
        
        Args:
            command: Command string (e.g., "SET key value", "GET key", "DELETE key")
        
        Returns:
            Result of the operation
        """
        parts = command.split()
        
        if len(parts) == 0:
            return "ERROR: Empty command"
        
        operation = parts[0].upper()
        
        try:
            if operation == 'SET' and len(parts) >= 3:
                return self._set(parts[1], ' '.join(parts[2:]))
            
            elif operation == 'GET' and len(parts) == 2:
                return self._get(parts[1])
            
            elif operation == 'DELETE' and len(parts) == 2:
                return self._delete(parts[1])
            
            else:
                return f"ERROR: Invalid command format: {command}"
        
        except Exception as e:
            logger.error(f"Error applying command '{command}': {e}")
            return f"ERROR: {e}"
    
    def _set(self, key: str, value: str) -> str:
        """Set a key-value pair"""
        self.store[key] = value
        self._save_store()
        logger.debug(f"SET {key} = {value}")
        return f"OK: Set {key} = {value}"
    
    def _get(self, key: str) -> str:
        """Get value for a key"""
        value = self.store.get(key)
        if value is None:
            logger.debug(f"GET {key} = Not found")
            return f"ERROR: Key '{key}' not found"
        
        logger.debug(f"GET {key} = {value}")
        return value
    
    def _delete(self, key: str) -> str:
        """Delete a key"""
        if key in self.store:
            value = self.store.pop(key)
            self._save_store()
            logger.debug(f"DELETE {key} (was {value})")
            return f"OK: Deleted {key}"
        else:
            logger.debug(f"DELETE {key} = Not found")
            return f"ERROR: Key '{key}' not found"
    
    def get(self, key: str) -> Optional[str]:
        """Direct get (without logging command)"""
        return self.store.get(key)
    
    def set(self, key: str, value: str):
        """Direct set (without logging command)"""
        self.store[key] = value
        self._save_store()
    
    def delete(self, key: str) -> bool:
        """Direct delete (without logging command)"""
        if key in self.store:
            del self.store[key]
            self._save_store()
            return True
        return False
    
    def get_all(self) -> Dict[str, str]:
        """Get all key-value pairs"""
        return self.store.copy()
    
    def clear(self):
        """Clear all data (for testing)"""
        self.store = {}
        self._save_store()
        logger.warning("Cleared all key-value pairs")
    
    def size(self) -> int:
        """Get number of keys"""
        return len(self.store)
    
    def keys(self) -> list:
        """Get all keys"""
        return list(self.store.keys())
    
    def values(self) -> list:
        """Get all values"""
        return list(self.store.values())
    
    def __len__(self):
        """Return number of keys"""
        return len(self.store)
    
    def __repr__(self):
        """String representation"""
        return f"StateMachine(keys={len(self.store)})"