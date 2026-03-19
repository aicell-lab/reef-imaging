"""High-level configuration management.

This module provides ConfigManager, a high-level interface for managing
orchestrator configuration with async locking for thread safety.
"""
import asyncio
import copy
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .models import MicroscopeConfig, Sample, Task
from .store import ConfigStore

logger = logging.getLogger(__name__)


class ConfigManager:
    """High-level configuration manager with async locking.
    
    This class provides thread-safe access to configuration data with
    methods for loading, saving, and updating tasks and samples.
    
    All operations that modify or read configuration data acquire an
    internal lock to prevent race conditions.
    """
    
    def __init__(
        self,
        config_path: str = "config.json",
        config_read_interval: int = 10
    ):
        """Initialize the ConfigManager.
        
        Args:
            config_path: Path to the configuration file
            config_read_interval: Seconds to wait before re-reading config.json
        """
        self._store = ConfigStore(config_path)
        self._config_lock = asyncio.Lock()
        self._config_read_interval = config_read_interval
        self._last_read_time: Optional[float] = None
        
        # Cached configuration data
        self._tasks: Dict[str, Task] = {}
        self._microscopes: Dict[str, MicroscopeConfig] = {}
    
    @property
    def tasks(self) -> Dict[str, Task]:
        """Get the current tasks dictionary."""
        return self._tasks
    
    @property
    def microscopes(self) -> Dict[str, MicroscopeConfig]:
        """Get the current microscopes dictionary."""
        return self._microscopes
    
    async def load_config(self) -> Dict[str, Any]:
        """Load configuration from file.
        
        This is a low-level method that reads the raw configuration.
        For task-aware loading, use load_tasks().
        
        Returns:
            The raw configuration dictionary
        """
        async with self._config_lock:
            data = self._store.read_raw()
            self._last_read_time = asyncio.get_event_loop().time()
            return data
    
    async def load_tasks(self, existing_tasks: Optional[Dict[str, Task]] = None) -> Tuple[Dict[str, Task], List[str]]:
        """Load and update tasks from configuration file.
        
        This method parses samples from the configuration file into Task objects,
        updates existing tasks, and detects removed tasks.
        
        Args:
            existing_tasks: Optional dictionary of existing tasks to update.
                          If not provided, uses self._tasks.
                          
        Returns:
            Tuple of (updated_tasks, removed_task_names)
            
        Raises:
            May raise FileNotFoundError or json.JSONDecodeError on critical errors,
            but typically handles these gracefully with warnings.
        """
        if existing_tasks is None:
            existing_tasks = self._tasks
            
        async with self._config_lock:
            return await self._load_and_update_tasks_unlocked(existing_tasks)
    
    async def _load_and_update_tasks_unlocked(
        self, 
        existing_tasks: Dict[str, Task]
    ) -> Tuple[Dict[str, Task], List[str]]:
        """Internal method to load tasks (must be called with lock held)."""
        raw_config = self._store.read_raw()
        current_time_naive = datetime.now()
        
        new_task_configs: Dict[str, Sample] = {}
        
        # Parse samples from config
        for sample_data in raw_config.get("samples", []):
            task_name = sample_data.get("name")
            settings = sample_data.get("settings")
            
            if not task_name or not settings:
                logger.warning(
                    f"Found sample without name or settings. Skipping: {sample_data}"
                )
                continue
            
            try:
                sample = Sample.from_dict(sample_data)
                new_task_configs[task_name] = sample
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing sample {task_name}: {e}. Skipping.")
                continue
        
        # Track which tasks were removed
        tasks_to_remove = [
            name for name in existing_tasks 
            if name not in new_task_configs
        ]
        
        # Determine if any changes require config write
        a_task_state_changed_for_write = False
        
        # Process each task
        for task_name, sample in new_task_configs.items():
            # Find operational state from file for this sample
            operational_state_from_file = {}
            for sample_data in raw_config.get("samples", []):
                if sample_data.get("name") == task_name:
                    operational_state_from_file = sample_data.get("operational_state", {})
                    break
            
            persisted_status = operational_state_from_file.get("status", "pending")
            
            # Determine actual status based on pending datetimes
            current_actual_status = persisted_status
            if not sample.pending_datetimes:
                if persisted_status not in ["uploading", "paused"]:
                    current_actual_status = "completed"
            elif persisted_status == "completed" and sample.pending_datetimes:
                # Reset to pending if points were added back
                current_actual_status = "pending"
                logger.info(
                    f"Task '{task_name}' was completed but now has pending points. "
                    "Resetting status to pending."
                )
                a_task_state_changed_for_write = True
            
            if task_name not in existing_tasks:
                # New task
                logger.info(f"New task added: {task_name}")
                task = Task.from_sample(sample)
                task.status = current_actual_status
                existing_tasks[task_name] = task
                a_task_state_changed_for_write = True
            else:
                # Update existing task
                existing_task = existing_tasks[task_name]
                old_config = existing_task.config
                
                # Check for significant config changes
                config_changed_significantly = (
                    old_config.get("pending_datetimes") != sample.pending_datetimes or
                    old_config.get("imaged_datetimes") != sample.imaged_datetimes or
                    any(old_config.get(k) != getattr(sample, k)
                        for k in ["incubator_slot", "allocated_microscope", "wells_to_scan", "Nx", "Ny"])
                )
                
                # Update config
                existing_task.config = sample.get_config_dict()
                existing_task._raw_settings_from_input = copy.deepcopy(sample.settings)
                a_task_state_changed_for_write = True
                
                # Update status if changed
                if existing_task.status != current_actual_status:
                    logger.info(
                        f"Task '{task_name}' status changing from "
                        f"'{existing_task.status}' to '{current_actual_status}'"
                    )
                    existing_task.status = current_actual_status
                
                # Handle significant changes during active processing
                if config_changed_significantly and existing_task.status not in ["pending", "completed", "paused"]:
                    if sample.pending_datetimes:
                        if existing_task.status != "pending":
                            logger.info(
                                f"Task '{task_name}' had significant config changes while "
                                f"in status '{existing_task.status}'. Resetting to pending."
                            )
                            existing_task.status = "pending"
                    elif not sample.imaged_datetimes:
                        if existing_task.status != "completed":
                            logger.info(
                                f"Task '{task_name}' had significant config changes. "
                                "No pending points. Marking completed."
                            )
                            existing_task.status = "completed"
            
            # Final status consistency check
            task = existing_tasks.get(task_name)
            if task and not task.config.get("pending_datetimes"):
                if task.status not in ["completed", "uploading", "paused"]:
                    logger.warning(
                        f"Task '{task_name}' has status '{task.status}' but no pending "
                        f"time points. Forcing to 'completed'."
                    )
                    task.status = "completed"
                    a_task_state_changed_for_write = True
        
        # Load microscope configurations
        newly_configured_microscopes: Dict[str, MicroscopeConfig] = {}
        for mic_config in raw_config.get("microscopes", []):
            mic_id = mic_config.get("id")
            if mic_id:
                newly_configured_microscopes[mic_id] = MicroscopeConfig.from_dict(mic_config)
            else:
                logger.warning(
                    f"Found microscope configuration without ID. Skipping: {mic_config}"
                )
        
        self._microscopes = newly_configured_microscopes
        self._last_read_time = asyncio.get_event_loop().time()
        
        return existing_tasks, tasks_to_remove, a_task_state_changed_for_write
    
    async def save_tasks(self, tasks: Optional[Dict[str, Task]] = None) -> bool:
        """Save tasks to configuration file.
        
        Args:
            tasks: Dictionary of tasks to save. If None, uses self._tasks.
            
        Returns:
            True if save was successful, False otherwise
        """
        if tasks is None:
            tasks = self._tasks
            
        async with self._config_lock:
            return await self._save_tasks_unlocked(tasks)
    
    async def _save_tasks_unlocked(self, tasks: Dict[str, Task]) -> bool:
        """Internal method to save tasks (must be called with lock held)."""
        # Read existing non-sample data to preserve it
        output_config: Dict[str, Any] = {"samples": []}
        
        existing_data = self._store.read()
        if existing_data:
            for key, value in existing_data.items():
                if key != "samples":
                    output_config[key] = value
        
        # Convert tasks to sample entries
        for task_name, task in tasks.items():
            # Reconstruct Sample from Task for serialization
            sample_entry = self._task_to_sample_entry(task)
            output_config["samples"].append(sample_entry)
        
        # Write atomically
        return self._store.write(output_config)
    
    def _task_to_sample_entry(self, task: Task) -> Dict[str, Any]:
        """Convert a Task back to config.json format."""
        settings_to_write = copy.deepcopy(task._raw_settings_from_input or {})
        config = task.config
        
        # Ensure critical fields are preserved
        settings_to_write["scan_mode"] = config.get("scan_mode", "full_automation")
        settings_to_write["saved_data_type"] = config.get("saved_data_type", "raw_images_well_plate")
        
        critical_fields = [
            "incubator_slot", "allocated_microscope",
            "wells_to_scan", "Nx", "Ny", "dx", "dy", "well_plate_type", "positions",
            "illumination_settings", "do_contrast_autofocus", "do_reflection_af",
            "focus_map_points", "move_for_autofocus"
        ]
        
        for field in critical_fields:
            if field in config:
                settings_to_write[field] = copy.deepcopy(config[field])
        
        # Update time points
        pending_datetimes = config.get("pending_datetimes", [])
        imaged_datetimes = config.get("imaged_datetimes", [])
        
        settings_to_write["pending_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(dt, datetime) else dt
            for dt in pending_datetimes
        ])
        settings_to_write["imaged_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(dt, datetime) else dt
            for dt in imaged_datetimes
        ])
        
        # Update imaging flags
        has_pending = bool(pending_datetimes)
        has_imaged = bool(imaged_datetimes)
        settings_to_write["imaging_completed"] = not has_pending
        settings_to_write["imaging_started"] = has_imaged or (not has_pending and has_imaged)
        
        return {
            "name": task.name,
            "settings": settings_to_write,
            "operational_state": {
                "status": task.status,
                "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
        }
    
    async def update_task_state(
        self,
        task_name: str,
        status: Optional[str] = None,
        current_tp_to_move_to_imaged: Optional[datetime] = None,
        tasks: Optional[Dict[str, Task]] = None
    ) -> bool:
        """Update task state and persist to configuration.
        
        Args:
            task_name: Name of the task to update
            status: New status (optional)
            current_tp_to_move_to_imaged: Time point to move from pending to imaged (optional)
            tasks: Dictionary of tasks. If None, uses self._tasks.
            
        Returns:
            True if update was successful, False otherwise
        """
        if tasks is None:
            tasks = self._tasks
            
        async with self._config_lock:
            return await self._update_task_state_unlocked(
                task_name, status, current_tp_to_move_to_imaged, tasks
            )
    
    async def _update_task_state_unlocked(
        self,
        task_name: str,
        status: Optional[str],
        current_tp_to_move_to_imaged: Optional[datetime],
        tasks: Dict[str, Task]
    ) -> bool:
        """Internal method to update task state (must be called with lock held)."""
        if task_name not in tasks:
            logger.warning(f"update_task_state: Task {task_name} not found.")
            return False
        
        changed = False
        task = tasks[task_name]
        config = task.config
        
        if status and task.status != status:
            logger.info(f"Task '{task_name}' status changing from '{task.status}' to '{status}'")
            task.status = status
            changed = True
        
        if current_tp_to_move_to_imaged:
            pending = config.get("pending_datetimes", [])
            imaged = config.get("imaged_datetimes", [])
            
            if current_tp_to_move_to_imaged in pending:
                pending.remove(current_tp_to_move_to_imaged)
                imaged.append(current_tp_to_move_to_imaged)
                imaged.sort()
                config["pending_datetimes"] = pending
                config["imaged_datetimes"] = imaged
                logger.info(
                    f"Moved time point {current_tp_to_move_to_imaged.isoformat()} "
                    f"to imaged for task '{task_name}'."
                )
                changed = True
            else:
                logger.warning(
                    f"Time point {current_tp_to_move_to_imaged.isoformat()} not found "
                    f"in pending_datetimes for task '{task_name}'."
                )
        
        # Auto-update status based on pending points
        pending = config.get("pending_datetimes", [])
        if not pending:
            if task.status not in ["completed", "uploading"]:
                logger.info(f"Task '{task_name}' has no more pending time points. Marking as completed.")
                task.status = "completed"
                changed = True
        elif status == "completed" and pending:
            logger.warning(
                f"Task '{task_name}' set to completed, but still has pending points. "
                "Reverting to pending."
            )
            task.status = "pending"
            changed = True
        
        if changed:
            return await self._save_tasks_unlocked(tasks)
        
        return True
    
    async def add_task(
        self,
        task_name: str,
        settings: Dict[str, Any],
        status: str = "pending",
        tasks: Optional[Dict[str, Task]] = None
    ) -> bool:
        """Add a new task to the configuration.
        
        Args:
            task_name: Name of the task
            settings: Task settings dictionary
            status: Initial status
            tasks: Dictionary of tasks. If None, uses self._tasks.
            
        Returns:
            True if task was added successfully, False otherwise
        """
        if tasks is None:
            tasks = self._tasks
            
        async with self._config_lock:
            # Read current config
            config_data = self._store.read_raw()
            
            # Ensure samples list exists
            if "samples" not in config_data or not isinstance(config_data["samples"], list):
                config_data["samples"] = []
            
            # Check if task already exists
            existing_index = -1
            for i, sample in enumerate(config_data["samples"]):
                if sample.get("name") == task_name:
                    existing_index = i
                    break
            
            # Prepare operational state
            op_state = {
                "status": status,
                "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
            
            new_entry = {
                "name": task_name,
                "settings": settings,
                "operational_state": op_state
            }
            
            if existing_index >= 0:
                config_data["samples"][existing_index] = new_entry
                logger.info(f"Task '{task_name}' updated.")
            else:
                config_data["samples"].append(new_entry)
                logger.info(f"Task '{task_name}' added.")
            
            # Write config
            success = self._store.write(config_data)
            
            # Update in-memory tasks
            if success:
                sample = Sample.from_dict(new_entry)
                tasks[task_name] = Task.from_sample(sample)
            
            return success
    
    async def delete_task(
        self,
        task_name: str,
        tasks: Optional[Dict[str, Task]] = None
    ) -> bool:
        """Delete a task from the configuration.
        
        Args:
            task_name: Name of the task to delete
            tasks: Dictionary of tasks. If None, uses self._tasks.
            
        Returns:
            True if task was deleted successfully, False otherwise
        """
        if tasks is None:
            tasks = self._tasks
            
        async with self._config_lock:
            # Read current config
            config_data = self._store.read_raw()
            
            if "samples" not in config_data or not isinstance(config_data["samples"], list):
                logger.warning("No samples list in configuration.")
                return False
            
            original_count = len(config_data["samples"])
            config_data["samples"] = [
                s for s in config_data["samples"] 
                if s.get("name") != task_name
            ]
            
            if len(config_data["samples"]) == original_count:
                logger.warning(f"Task '{task_name}' not found.")
                return False
            
            # Write config
            success = self._store.write(config_data)
            
            # Update in-memory tasks
            if success and task_name in tasks:
                del tasks[task_name]
            
            logger.info(f"Task '{task_name}' deleted.")
            return success
    
    async def get_all_samples(self) -> List[Dict[str, Any]]:
        """Get all sample configurations from the file.
        
        Returns:
            List of sample dictionaries
        """
        async with self._config_lock:
            config_data = self._store.read_raw()
            return config_data.get("samples", [])
    
    def should_reload(self, current_time: Optional[float] = None) -> bool:
        """Check if configuration should be reloaded based on interval.

        Args:
            current_time: Current time from asyncio loop. If None, uses current time.

        Returns:
            True if enough time has passed to reload configuration
        """
        if current_time is None:
            try:
                current_time = asyncio.get_event_loop().time()
            except RuntimeError:
                return True  # No event loop, allow reload

        if self._last_read_time is None:
            return True

        return (current_time - self._last_read_time) > self._config_read_interval

    # =========================================================================
    # Compatibility Methods for Legacy Orchestrator Task Format
    # =========================================================================
    # These methods support the original orchestrator task structure:
    #   {
    #       "config": {parsed_settings},
    #       "status": "pending",
    #       "_raw_settings_from_input": {...}
    #   }

    async def load_tasks_compat(
        self,
        existing_tasks: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[str], bool, Dict[str, Any], Dict[str, Any]]:
        """Load tasks using legacy orchestrator format for backward compatibility.

        Args:
            existing_tasks: Optional dictionary of existing tasks to update.
                          If not provided, uses an empty dict.

        Returns:
            Tuple of (removed_task_names, state_changed, updated_tasks_dict, microscopes_dict)
        """
        if existing_tasks is None:
            existing_tasks = {}

        async with self._config_lock:
            raw_config = self._store.read_raw()

            # Parse all samples
            new_samples: Dict[str, Sample] = {}
            for sample_data in raw_config.get("samples", []):
                task_name = sample_data.get("name")
                if not task_name:
                    continue
                try:
                    sample = Sample.from_dict(sample_data)
                    new_samples[task_name] = sample
                except (KeyError, ValueError) as e:
                    logger.error(f"Error parsing sample {task_name}: {e}. Skipping.")
                    continue

            # Track changes
            removed_tasks = [name for name in existing_tasks if name not in new_samples]
            state_changed = False

            for task_name, sample in new_samples.items():
                # Find operational state from file
                op_state = {}
                for s in raw_config.get("samples", []):
                    if s.get("name") == task_name:
                        op_state = s.get("operational_state", {})
                        break

                persisted_status = op_state.get("status", "pending")

                # Determine actual status
                actual_status = persisted_status
                if not sample.pending_datetimes:
                    if persisted_status not in ["uploading", "paused"]:
                        actual_status = "completed"
                elif persisted_status == "completed" and sample.pending_datetimes:
                    actual_status = "pending"
                    state_changed = True

                if task_name not in existing_tasks:
                    # New task
                    logger.info(f"New task added: {task_name}")
                    existing_tasks[task_name] = {
                        "config": sample.get_config_dict(),
                        "status": actual_status,
                        "_raw_settings_from_input": copy.deepcopy(sample.settings)
                    }
                    state_changed = True
                else:
                    # Update existing task
                    existing = existing_tasks[task_name]
                    old_config = existing["config"]
                    new_config = sample.get_config_dict()

                    # Check for significant changes
                    config_changed = (
                        old_config.get("pending_datetimes") != new_config.get("pending_datetimes") or
                        old_config.get("imaged_datetimes") != new_config.get("imaged_datetimes") or
                        any(old_config.get(k) != new_config.get(k)
                            for k in ["incubator_slot", "allocated_microscope", "wells_to_scan", "Nx", "Ny"])
                    )

                    existing["config"] = new_config
                    existing["_raw_settings_from_input"] = copy.deepcopy(sample.settings)
                    state_changed = True

                    if existing["status"] != actual_status:
                        logger.info(
                            f"Task '{task_name}' status changing from "
                            f"'{existing['status']}' to '{actual_status}'"
                        )
                        existing["status"] = actual_status

                    # Handle config changes during active processing
                    if config_changed and existing["status"] not in ["pending", "completed", "paused"]:
                        if new_config.get("pending_datetimes"):
                            if existing["status"] != "pending":
                                logger.info(f"Task '{task_name}' config changed. Resetting to pending.")
                                existing["status"] = "pending"
                        elif not new_config.get("imaged_datetimes"):
                            if existing["status"] != "completed":
                                logger.info(f"Task '{task_name}' config changed. Marking completed.")
                                existing["status"] = "completed"

                # Final consistency check
                task = existing_tasks.get(task_name)
                if task and not task["config"].get("pending_datetimes"):
                    if task["status"] not in ["completed", "uploading", "paused"]:
                        logger.warning(
                            f"Task '{task_name}' has status '{task['status']}' "
                            f"but no pending points. Forcing to 'completed'."
                        )
                        task["status"] = "completed"
                        state_changed = True

            # Load microscope configurations
            microscopes = {}
            for mic_config in raw_config.get("microscopes", []):
                mic_id = mic_config.get("id")
                if mic_id:
                    microscopes[mic_id] = mic_config

            self._microscopes = {mid: MicroscopeConfig(id=mid) for mid in microscopes}
            self._last_read_time = asyncio.get_event_loop().time()

            return removed_tasks, state_changed, existing_tasks, microscopes

    async def save_tasks_compat(self, tasks: Dict[str, Any]) -> bool:
        """Save tasks in legacy format to configuration file.

        Args:
            tasks: Dictionary of tasks in legacy format

        Returns:
            True if save was successful, False otherwise
        """
        async with self._config_lock:
            # Read existing non-sample data to preserve it
            output_config: Dict[str, Any] = {"samples": []}

            existing_data = self._store.read()
            if existing_data:
                for key, value in existing_data.items():
                    if key != "samples":
                        output_config[key] = value

            # Convert legacy tasks to sample entries
            for task_name, task_data in tasks.items():
                sample_entry = self._legacy_task_to_sample_entry(task_name, task_data)
                output_config["samples"].append(sample_entry)

            # Write atomically
            return self._store.write(output_config)

    def _legacy_task_to_sample_entry(
        self,
        task_name: str,
        task_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert a legacy task dict back to config.json format."""
        raw_settings = copy.deepcopy(task_data.get("_raw_settings_from_input", {}))
        config = task_data.get("config", {})

        # Ensure critical fields are preserved
        raw_settings["scan_mode"] = config.get("scan_mode", "full_automation")
        raw_settings["saved_data_type"] = config.get("saved_data_type", "raw_images_well_plate")

        critical_fields = [
            "incubator_slot", "allocated_microscope",
            "wells_to_scan", "Nx", "Ny", "dx", "dy", "well_plate_type", "positions",
            "illumination_settings", "do_contrast_autofocus", "do_reflection_af",
            "focus_map_points", "move_for_autofocus"
        ]

        for field in critical_fields:
            if field in config:
                raw_settings[field] = copy.deepcopy(config[field])

        # Update time points
        pending_datetimes = config.get("pending_datetimes", [])
        imaged_datetimes = config.get("imaged_datetimes", [])

        raw_settings["pending_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(dt, datetime) else str(dt)
            for dt in pending_datetimes
        ])
        raw_settings["imaged_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') if isinstance(dt, datetime) else str(dt)
            for dt in imaged_datetimes
        ])

        # Update imaging flags
        has_pending = bool(pending_datetimes)
        has_imaged = bool(imaged_datetimes)
        raw_settings["imaging_completed"] = not has_pending
        raw_settings["imaging_started"] = has_imaged or (not has_pending and has_imaged)

        return {
            "name": task_name,
            "settings": raw_settings,
            "operational_state": {
                "status": task_data.get("status", "pending"),
                "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
        }

    async def update_task_state_compat(
        self,
        task_name: str,
        tasks: Dict[str, Any],
        status: Optional[str] = None,
        current_tp_to_move_to_imaged: Optional[datetime] = None
    ) -> bool:
        """Update task state in legacy format and persist to configuration.

        Args:
            task_name: Name of the task to update
            tasks: Dictionary of tasks in legacy format
            status: New status (optional)
            current_tp_to_move_to_imaged: Time point to move from pending to imaged (optional)

        Returns:
            True if update was successful, False otherwise
        """
        if task_name not in tasks:
            logger.warning(f"update_task_state_compat: Task {task_name} not found.")
            return False

        async with self._config_lock:
            return await self._update_task_state_compat_unlocked(
                task_name, tasks, status, current_tp_to_move_to_imaged
            )

    async def _update_task_state_compat_unlocked(
        self,
        task_name: str,
        tasks: Dict[str, Any],
        status: Optional[str],
        current_tp_to_move_to_imaged: Optional[datetime]
    ) -> bool:
        """Internal method (must be called with lock held)."""
        changed = False
        task = tasks[task_name]
        config = task["config"]

        if status and task.get("status") != status:
            logger.info(
                f"Task '{task_name}' status changing from "
                f"'{task.get('status')}' to '{status}'"
            )
            task["status"] = status
            changed = True

        if current_tp_to_move_to_imaged:
            pending = config.get("pending_datetimes", [])
            imaged = config.get("imaged_datetimes", [])

            if current_tp_to_move_to_imaged in pending:
                pending.remove(current_tp_to_move_to_imaged)
                imaged.append(current_tp_to_move_to_imaged)
                imaged.sort()
                config["pending_datetimes"] = pending
                config["imaged_datetimes"] = imaged
                logger.info(
                    f"Moved time point {current_tp_to_move_to_imaged.isoformat()} "
                    f"to imaged for task '{task_name}'."
                )
                changed = True
            else:
                logger.warning(
                    f"Time point {current_tp_to_move_to_imaged.isoformat()} not found "
                    f"in pending_datetimes for task '{task_name}'."
                )

        # Auto-update status based on pending points
        pending = config.get("pending_datetimes", [])
        if not pending:
            if task.get("status") not in ["completed", "uploading"]:
                logger.info(
                    f"Task '{task_name}' has no more pending time points. "
                    f"Marking as completed."
                )
                task["status"] = "completed"
                changed = True
        elif status == "completed" and pending:
            logger.warning(
                f"Task '{task_name}' set to completed, but still has pending points. "
                "Reverting to pending."
            )
            task["status"] = "pending"
            changed = True

        if changed:
            return await self.save_tasks_compat(tasks)

        return True
