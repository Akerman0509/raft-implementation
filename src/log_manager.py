"""
Log Manager for RAFT
Quản lý log entries với persistent storage
"""

import json
import logging
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class LogManager:
    """
    Quản lý RAFT log entries
    
    Features:
    - Append entries
    - Get entries
    - Check consistency
    - Persistent storage
    """
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.log_file = data_dir / 'raft_log.json'
        self.logs: List[Dict] = []
        
        # Load existing logs
        self._load_logs()
        
        logger.info(f"LogManager initialized with {len(self.logs)} entries")
    
    def _load_logs(self):
        """Load logs from persistent storage"""
        if self.log_file.exists():
            try:
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                    self.logs = data.get('logs', [])
                logger.info(f"Loaded {len(self.logs)} log entries from {self.log_file}")
            except Exception as e:
                logger.error(f"Failed to load logs: {e}")
                self.logs = []
        else:
            self.logs = []
            logger.info("No existing log file, starting with empty log")
    
    def _save_logs(self):
        """Save logs to persistent storage"""
        try:
            with open(self.log_file, 'w') as f:
                json.dump({'logs': self.logs}, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save logs: {e}")
    
    def append_entry(self, entry: Dict):
        """Append a single entry to log"""
        self.logs.append(entry)
        self._save_logs()
        logger.debug(f"Appended entry at index {entry['index']}")
    
    def append_entries(self, prev_log_index: int, entries: List[Dict]):
        """
        Append multiple entries to log
        
        Args:
            prev_log_index: Index of log entry immediately preceding new ones
            entries: List of log entries to append
        """
        # Remove conflicting entries
        if prev_log_index < len(self.logs):
            self.logs = self.logs[:prev_log_index]
        
        # Append new entries
        for entry in entries:
            self.logs.append(entry)
        
        self._save_logs()
        logger.info(f"Appended {len(entries)} entries after index {prev_log_index}")
    
    def get_entry(self, index: int) -> Optional[Dict]:
        """Get log entry at specific index (1-indexed)"""
        if index <= 0 or index > len(self.logs):
            return None
        return self.logs[index - 1]
    
    def get_entries_from(self, start_index: int) -> List[Dict]:
        """Get all entries from start_index onwards"""
        if start_index <= 0:
            return []
        
        if start_index > len(self.logs):
            return []
        
        return self.logs[start_index - 1:]
    
    def get_last_log_index(self) -> int:
        """Get index of last log entry"""
        return len(self.logs)
    
    def get_last_log_term(self) -> int:
        """Get term of last log entry"""
        if len(self.logs) == 0:
            return 0
        last_entry = self.logs[-1]
        
        if isinstance(last_entry, dict):
            return last_entry.get('term', 0)
        else:
            return getattr(last_entry, 'term', 0)
    
    def get_log_term(self, index: int) -> int:
        """Get term of log entry at specific index"""
        if index <= 0 or index > len(self.logs):
            return 0
        return self.logs[index - 1]['term']
    
    def check_log_consistency(self, prev_log_index: int, prev_log_term: int) -> bool:
        """
        Check if log contains entry at prev_log_index with term prev_log_term
        
        Returns:
            True if consistent, False otherwise
        """
        # Special case: empty log
        if prev_log_index == 0:
            return True
        
        # Check if we have entry at prev_log_index
        if prev_log_index > len(self.logs):
            return False
        
        # Check if term matches
        if self.logs[prev_log_index - 1]['term'] != prev_log_term:
            return False
        
        return True
    
    def get_all_entries(self) -> List[Dict]:
        """Get all log entries"""
        return self.logs.copy()
    
    def clear(self):
        """Clear all logs (for testing)"""
        self.logs = []
        self._save_logs()
        logger.warning("Cleared all log entries")
    
    def __len__(self):
        """Return number of log entries"""
        return len(self.logs)
    
    def __repr__(self):
        """String representation"""
        return f"LogManager(entries={len(self.logs)}, last_term={self.get_last_log_term()})"