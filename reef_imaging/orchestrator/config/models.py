"""Data models for orchestrator configuration.

This module contains dataclasses representing the configuration entities
used by the orchestrator system.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class MicroscopeConfig:
    """Configuration for a microscope device."""
    id: str
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MicroscopeConfig":
        """Create a MicroscopeConfig from a dictionary."""
        return cls(id=data["id"])
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for JSON serialization."""
        return {"id": self.id}


@dataclass
class Sample:
    """Represents a sample/task configuration.
    
    This encapsulates both the user-defined settings and the operational
    state maintained by the orchestrator.
    """
    name: str
    settings: Dict[str, Any]
    operational_state: Dict[str, Any] = field(default_factory=dict)
    
    # Internal parsed configuration (populated by ConfigManager)
    scan_mode: str = "full_automation"
    saved_data_type: str = "raw_images_well_plate"
    allocated_microscope: str = "microscope-squid-1"
    scan_timeout_minutes: int = 120
    incubator_slot: Optional[int] = None
    wells_to_scan: List[str] = field(default_factory=list)
    Nx: int = 0
    Ny: int = 0
    dx: float = 0.8
    dy: float = 0.8
    well_plate_type: str = "96"
    positions: List[Dict[str, Any]] = field(default_factory=list)
    illumination_settings: List[Dict[str, Any]] = field(default_factory=list)
    do_contrast_autofocus: bool = False
    do_reflection_af: bool = False
    focus_map_points: Optional[List[List[float]]] = None
    move_for_autofocus: Optional[bool] = None
    
    # Runtime state
    pending_datetimes: List[datetime] = field(default_factory=list)
    imaged_datetimes: List[datetime] = field(default_factory=list)
    imaging_started: bool = False
    imaging_completed: bool = False
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Sample":
        """Create a Sample from a dictionary (config.json format)."""
        sample = cls(
            name=data.get("name", ""),
            settings=data.get("settings", {}),
            operational_state=data.get("operational_state", {})
        )
        
        # Parse settings into typed fields
        sample._parse_settings()
        return sample
    
    def _parse_settings(self) -> None:
        """Parse raw settings into typed configuration fields."""
        settings = self.settings
        
        self.scan_mode = settings.get("scan_mode", "full_automation")
        self.saved_data_type = settings.get("saved_data_type", "raw_images_well_plate")
        self.allocated_microscope = settings.get("allocated_microscope", "microscope-squid-1")
        self.scan_timeout_minutes = settings.get("scan_timeout_minutes", 120)
        
        if self.scan_mode == "full_automation":
            self.incubator_slot = settings.get("incubator_slot")
        
        # Parse time points
        self.pending_datetimes = []
        for tp_str in settings.get("pending_time_points", []):
            try:
                self.pending_datetimes.append(datetime.fromisoformat(tp_str))
            except ValueError:
                pass  # Skip invalid datetime strings
        self.pending_datetimes.sort()
        
        self.imaged_datetimes = []
        for tp_str in settings.get("imaged_time_points", []):
            try:
                self.imaged_datetimes.append(datetime.fromisoformat(tp_str))
            except ValueError:
                pass  # Skip invalid datetime strings
        self.imaged_datetimes.sort()
        
        # Determine imaging flags
        has_pending = bool(self.pending_datetimes)
        has_imaged = bool(self.imaged_datetimes)
        self.imaging_completed = not has_pending
        self.imaging_started = has_imaged or (not has_pending and has_imaged)
        
        # Parse imaging settings
        self.illumination_settings = settings.get("illumination_settings", [])
        self.do_contrast_autofocus = settings.get("do_contrast_autofocus", False)
        self.do_reflection_af = settings.get("do_reflection_af", False)
        
        # Parse data type specific settings
        if self.saved_data_type == "raw_images_well_plate":
            self.wells_to_scan = settings.get("wells_to_scan", [])
            self.Nx = settings.get("Nx", 0)
            self.Ny = settings.get("Ny", 0)
            self.dx = settings.get("dx", 0.8)
            self.dy = settings.get("dy", 0.8)
            self.well_plate_type = settings.get("well_plate_type", "96")
        else:
            self.positions = settings.get("positions", [])
        
        # Optional fields
        self.focus_map_points = settings.get("focus_map_points")
        self.move_for_autofocus = settings.get("move_for_autofocus")
    
    def to_config_entry(self) -> Dict[str, Any]:
        """Convert to config.json format for serialization."""
        # Update settings with current state
        settings = dict(self.settings)
        
        # Always include critical fields
        settings["scan_mode"] = self.scan_mode
        settings["saved_data_type"] = self.saved_data_type
        settings["allocated_microscope"] = self.allocated_microscope
        
        # Critical fields that must be preserved
        critical_fields = [
            "incubator_slot", "wells_to_scan", "Nx", "Ny", 
            "dx", "dy", "well_plate_type", "positions",
            "illumination_settings", "do_contrast_autofocus", "do_reflection_af",
            "focus_map_points", "move_for_autofocus"
        ]
        
        for field_name in critical_fields:
            value = getattr(self, field_name)
            if value is not None:
                settings[field_name] = value
        
        # Update time points
        settings["pending_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in self.pending_datetimes
        ])
        settings["imaged_time_points"] = sorted([
            dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in self.imaged_datetimes
        ])
        
        # Update imaging flags
        has_pending = bool(self.pending_datetimes)
        has_imaged = bool(self.imaged_datetimes)
        settings["imaging_completed"] = not has_pending
        settings["imaging_started"] = has_imaged or (not has_pending and has_imaged)
        
        return {
            "name": self.name,
            "settings": settings,
            "operational_state": {
                "status": self.operational_state.get("status", "pending"),
                "last_updated_by_orchestrator": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
        }
    
    def get_config_dict(self) -> Dict[str, Any]:
        """Get the configuration dict used by the orchestrator runtime."""
        config = {
            "name": self.name,
            "scan_mode": self.scan_mode,
            "saved_data_type": self.saved_data_type,
            "allocated_microscope": self.allocated_microscope,
            "scan_timeout_minutes": self.scan_timeout_minutes,
            "illumination_settings": self.illumination_settings,
            "do_contrast_autofocus": self.do_contrast_autofocus,
            "do_reflection_af": self.do_reflection_af,
            "pending_datetimes": self.pending_datetimes,
            "imaged_datetimes": self.imaged_datetimes,
            "imaging_started": self.imaging_started,
            "imaging_completed": self.imaging_completed,
        }
        
        if self.scan_mode == "full_automation":
            config["incubator_slot"] = self.incubator_slot
        
        if self.saved_data_type == "raw_images_well_plate":
            config.update({
                "wells_to_scan": self.wells_to_scan,
                "Nx": self.Nx,
                "Ny": self.Ny,
                "dx": self.dx,
                "dy": self.dy,
                "well_plate_type": self.well_plate_type,
            })
        else:
            config["positions"] = self.positions
        
        if self.focus_map_points is not None:
            config["focus_map_points"] = self.focus_map_points
        if self.move_for_autofocus is not None:
            config["move_for_autofocus"] = self.move_for_autofocus
            
        return config


@dataclass  
class Task:
    """Runtime task state maintained by the orchestrator.
    
    This combines the sample configuration with runtime state like
    current status and raw settings backup.
    """
    name: str
    config: Dict[str, Any]  # The parsed config dict used by runtime
    status: str = "pending"
    _raw_settings_from_input: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_sample(cls, sample: Sample) -> "Task":
        """Create a Task from a Sample."""
        return cls(
            name=sample.name,
            config=sample.get_config_dict(),
            status=sample.operational_state.get("status", "pending"),
            _raw_settings_from_input=dict(sample.settings)
        )
