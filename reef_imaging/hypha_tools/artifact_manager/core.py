import os
import asyncio
import aiohttp
from hypha_rpc import connect_to_server
from dotenv import load_dotenv
import json
import random
from datetime import datetime
from typing import Dict, Set, Any, Tuple, Optional, List, Union

# Load environment variables
load_dotenv()

class Config:
    """Configuration settings for the artifact manager"""
    SERVER_URL = "https://hypha.aicell.io"
    WORKSPACE_TOKEN = os.getenv("AGENT_LENS_WORKSPACE_TOKEN")
    CONCURRENCY_LIMIT = 25  # Max number of concurrent uploads (increased from 10)
    MAX_RETRIES = 300  # Maximum number of retry attempts
    MAX_RETRIES_PER_FILE = 10  # Maximum number of retry attempts per file
    INITIAL_RETRY_DELAY = 5  # Initial retry delay in seconds
    MAX_RETRY_DELAY = 60  # Maximum retry delay in seconds
    CONNECTION_TIMEOUT = 30  # Timeout for API connections in seconds
    UPLOAD_TIMEOUT = 3600  # Timeout for file uploads in seconds (increased from 60)
    URL_BATCH_SIZE = 30  # Number of presigned URLs to request at once
    MAX_WORKERS = 20  # Maximum number of worker tasks
    MIN_URL_WORKERS = 4  # Minimum number of URL workers
    CONNECTION_POOL_SIZE = 100  # TCP connection pool size
    MAX_COMMIT_ATTEMPTS = 15  # Maximum number of commit attempts
    MAX_COMMIT_DELAY = 220  # Maximum delay between commit attempts in seconds
class UploadRecord:
    """Manages the record of uploaded files"""
    
    def __init__(self):
        self.uploaded_files: Set[str] = set()
        self.last_update: Optional[str] = None
        self.total_files: int = 0
        self.completed_files: int = 0
    
    
    def mark_uploaded(self, relative_path: str) -> None:
        """Mark a file as uploaded"""
        self.uploaded_files.add(relative_path)
        self.completed_files += 1
        
        # Save progress periodically (every 10 files)
        if self.completed_files % 10 == 0:
            self.save()
    
    def set_total_files(self, total: int) -> None:
        """Set the total number of files to upload"""
        self.total_files = total
        self.save()

class HyphaConnection:
    """Manages connections to the Hypha server"""
    
    def __init__(self, server_url: str = Config.SERVER_URL, token: str = Config.WORKSPACE_TOKEN):
        self.server_url = server_url
        self.token = token
        self.api = None
        self.artifact_manager = None
    
    async def connect(self, timeout: int = Config.CONNECTION_TIMEOUT, client_id: str = "reef-client") -> None:
        """Connect to the Hypha server with robust error handling"""
        # Always attempt to disconnect first to clear any lingering state
        await self.disconnect()
        
        try:
            print(f"Attempting connection to {self.server_url} with client_id: {client_id}")
            self.api = await asyncio.wait_for(
                connect_to_server({
                    "client_id": client_id, 
                    "server_url": self.server_url, 
                    "token": self.token,
                }),
                timeout=timeout
            )
            print("Connection established, getting artifact manager...")
            self.artifact_manager = await asyncio.wait_for(
                self.api.get_service("public/artifact-manager"),
                timeout=timeout
            )
            print("Connected successfully to Hypha and artifact manager")
        except asyncio.TimeoutError:
            print(f"Connection attempt timed out after {timeout} seconds")
            # Ensure cleanup even on timeout during connection or service retrieval
            await self.disconnect() 
            raise
        except Exception as e:
            # Catch specific errors if possible, e.g., check 'Client already exists'
            error_msg = str(e)
            print(f"Connection error: {error_msg}")
            if "Client already exists" in error_msg:
                 print("Client ID conflict detected. Ensure only one instance is running or use unique client IDs.")
            # Ensure cleanup on any connection error
            await self.disconnect() 
            raise # Re-raise the exception after cleanup
    
    async def disconnect(self, timeout: int = 5) -> None:
        """Disconnect the connection to the Hypha server gracefully."""
        if self.api is not None:
            print("Disconnecting from Hypha server...")
            try:
                await asyncio.wait_for(self.api.disconnect(), timeout=timeout)
                print("Hypha API disconnected successfully.")
            except asyncio.TimeoutError:
                print(f"Hypha disconnect timed out after {timeout} seconds.")
            except Exception as e:
                print(f"Error during Hypha API disconnection: {e}")
            finally:
                # Always reset state variables after attempting disconnect
                self.api = None
                self.artifact_manager = None
        else:
            # Ensure state is clean even if api was already None
            self.api = None
            self.artifact_manager = None
            # print("Already disconnected or connection not established.") # Optional: uncomment for more verbose logging

    async def reconnect(self, timeout: int = Config.CONNECTION_TIMEOUT, client_id: str = "reef-client") -> None:
        """Reconnect to the Hypha server"""
        print("Attempting to reconnect...")
        await self.disconnect() # Ensure clean state before reconnecting
        await self.connect(timeout=timeout, client_id=client_id)
