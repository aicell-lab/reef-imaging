"""Configuration persistence layer.

This module provides atomic file operations for reading and writing
configuration data to JSON files.
"""
import json
import os
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ConfigStore:
    """Handles atomic read/write operations for configuration files.
    
    This class ensures that configuration writes are atomic by writing to
    a temporary file first, then renaming it to the target file. This prevents
    corruption if the process is interrupted during a write operation.
    """
    
    def __init__(
        self, 
        config_path: str = "config.json",
        temp_suffix: str = ".tmp"
    ):
        """Initialize the ConfigStore.
        
        Args:
            config_path: Path to the configuration file
            temp_suffix: Suffix for temporary files during atomic writes
        """
        self.config_path = Path(config_path)
        self.temp_path = self.config_path.with_suffix(temp_suffix)
    
    def read(self) -> Optional[Dict[str, Any]]:
        """Read configuration from file.
        
        Returns:
            The configuration dictionary, or None if file doesn't exist or is invalid.
        """
        if not self.config_path.exists():
            logger.warning(f"Configuration file {self.config_path} not found.")
            return None
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from {self.config_path}: {e}")
            return None
        except OSError as e:
            logger.error(f"Error reading {self.config_path}: {e}")
            return None
    
    def read_raw(self) -> Dict[str, Any]:
        """Read configuration, returning empty dict on error.
        
        Returns:
            The configuration dictionary, or {"samples": []} if file doesn't exist.
        """
        data = self.read()
        if data is None:
            return {"samples": []}
        return data
    
    def write(self, data: Dict[str, Any]) -> bool:
        """Write configuration to file atomically.
        
        Writes to a temporary file first, then renames it to the target file.
        This ensures that the configuration file is never in a partially written state.
        
        Args:
            data: The configuration dictionary to write
            
        Returns:
            True if write was successful, False otherwise
        """
        try:
            # Write to temporary file
            with open(self.temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            
            # Atomic rename
            os.replace(self.temp_path, self.config_path)
            
            logger.debug(f"Configuration written to {self.config_path}")
            return True
            
        except (IOError, OSError) as e:
            logger.error(f"Error writing configuration to {self.config_path}: {e}")
            return False
    
    def ensure_exists(self, default_data: Optional[Dict[str, Any]] = None) -> bool:
        """Ensure the configuration file exists, creating it if necessary.
        
        Args:
            default_data: Default configuration to write if file doesn't exist
            
        Returns:
            True if file exists or was created successfully
        """
        if self.config_path.exists():
            return True
        
        if default_data is None:
            default_data = {"samples": [], "microscopes": []}
        
        return self.write(default_data)
