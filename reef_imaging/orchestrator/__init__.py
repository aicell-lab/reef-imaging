"""Orchestrator package for reef-imaging.

This package contains the main orchestration logic and configuration management.
"""
from .main import OrchestrationSystem, setup_logging, main

__all__ = ["OrchestrationSystem", "setup_logging", "main"]
