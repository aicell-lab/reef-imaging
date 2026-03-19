"""Configuration management module for the orchestrator.

This module provides a clean separation between configuration persistence
and the orchestrator's business logic. It handles:

- Atomic file I/O for configuration files
- Type-safe configuration models using dataclasses
- Async-safe configuration management with locking
- Backward compatibility with existing config.json format

Example usage:
    from reef_imaging.orchestrator.config import ConfigManager
    
    config_manager = ConfigManager(config_path="config.json")
    
    # Load tasks
    tasks, removed = await config_manager.load_tasks()
    
    # Update task state
    await config_manager.update_task_state(
        task_name="my-experiment",
        status="active"
    )
    
    # Save all tasks
    await config_manager.save_tasks()
"""
from .models import MicroscopeConfig, Sample, Task
from .store import ConfigStore
from .manager import ConfigManager

__all__ = [
    "ConfigManager",
    "ConfigStore", 
    "MicroscopeConfig",
    "Sample",
    "Task",
]
