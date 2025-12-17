#!/usr/bin/env python
"""
Cleanup script to fix corrupted config.json by removing fields that shouldn't be persistent.
This script removes action_ID and fixes positions/wells_to_scan based on saved_data_type.
"""
import json
import copy

CONFIG_FILE = "config.json"

def cleanup_config():
    """Remove incorrectly shared fields from config.json"""
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    
    print(f"Found {len(config.get('samples', []))} tasks to clean up")
    
    for sample in config.get("samples", []):
        settings = sample.get("settings", {})
        task_name = sample.get("name", "unknown")
        saved_data_type = settings.get("saved_data_type", "raw_images_well_plate")
        
        # Remove action_ID if present (it's generated at runtime, not a config field)
        if "action_ID" in settings:
            print(f"  {task_name}: Removing action_ID")
            del settings["action_ID"]
        
        # Fix positions vs wells_to_scan based on saved_data_type
        if saved_data_type == "raw_images_well_plate":
            # Should have wells_to_scan, not positions
            if "positions" in settings and "wells_to_scan" in settings:
                print(f"  {task_name}: Removing positions field (raw_images_well_plate uses wells_to_scan)")
                del settings["positions"]
        elif saved_data_type == "raw_image_flexible":
            # Should have positions, not wells_to_scan
            if "wells_to_scan" in settings and "positions" in settings:
                print(f"  {task_name}: Keeping positions field (raw_image_flexible type)")
                # Keep positions, but could remove wells_to_scan if it shouldn't be there
    
    # Write cleaned config
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)
    
    print(f"\nCleanup complete! Config written to {CONFIG_FILE}")

if __name__ == "__main__":
    cleanup_config()

