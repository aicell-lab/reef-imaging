"""Standardized API response types for reef-imaging services."""

from pydantic import BaseModel, Field
from typing import Generic, TypeVar, Optional, Literal, Any, Dict, List

T = TypeVar('T')


class ApiResponse(BaseModel, Generic[T]):
    """
    Standardized API response format for all reef-imaging services.
    
    This generic response type ensures consistent response structure across
    all services (incubator, robotic arm, orchestrator, microscopes).
    
    Type Parameters:
        T: The type of data contained in a successful response
    
    Attributes:
        status: Either "success" or "error"
        data: The response data (None for error responses)
        message: Human-readable message (error description or success info)
        error_code: Machine-readable error code for error responses
    
    Example:
        # Success response
        response = ApiResponse.success(data={"temperature": 37.0})
        # {"status": "success", "data": {"temperature": 37.0}, "message": null, "error_code": null}
        
        # Error response  
        response = ApiResponse.error(message="Connection failed", error_code="CONN_FAILED")
        # {"status": "error", "data": null, "message": "Connection failed", "error_code": "CONN_FAILED"}
    """
    
    status: Literal["success", "error"] = Field(
        ...,
        description="Response status: 'success' or 'error'"
    )
    data: Optional[T] = Field(
        default=None,
        description="Response data for successful requests"
    )
    message: Optional[str] = Field(
        default=None,
        description="Human-readable message (success info or error description)"
    )
    error_code: Optional[str] = Field(
        default=None,
        description="Machine-readable error code for error responses"
    )
    
    @classmethod
    def success(cls, data: T = None, message: str = None) -> "ApiResponse[T]":
        """
        Create a successful API response.
        
        Args:
            data: The response data payload
            message: Optional human-readable success message
        
        Returns:
            ApiResponse with status="success"
        """
        return cls(status="success", data=data, message=message)
    
    @classmethod
    def error(cls, message: str, error_code: str = None, data: Any = None) -> "ApiResponse[Any]":
        """
        Create an error API response.
        
        Args:
            message: Human-readable error description
            error_code: Machine-readable error code (e.g., "CONN_FAILED", "NOT_FOUND")
            data: Optional additional error data
        
        Returns:
            ApiResponse with status="error"
        """
        return cls(status="error", message=message, error_code=error_code, data=data)
    
    def is_success(self) -> bool:
        """Check if the response indicates success."""
        return self.status == "success"
    
    def is_error(self) -> bool:
        """Check if the response indicates an error."""
        return self.status == "error"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert response to dictionary."""
        return {
            "status": self.status,
            "data": self.data,
            "message": self.message,
            "error_code": self.error_code
        }


# Common error codes for standardization
class ErrorCode:
    """Standard error codes for reef-imaging services."""
    
    # Connection errors
    CONN_FAILED = "CONN_FAILED"
    CONN_TIMEOUT = "CONN_TIMEOUT"
    CONN_REFUSED = "CONN_REFUSED"
    
    # Service errors
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    SERVICE_NOT_FOUND = "SERVICE_NOT_FOUND"
    SERVICE_BUSY = "SERVICE_BUSY"
    
    # Hardware errors
    HARDWARE_ERROR = "HARDWARE_ERROR"
    HARDWARE_TIMEOUT = "HARDWARE_TIMEOUT"
    HARDWARE_NOT_CONNECTED = "HARDWARE_NOT_CONNECTED"
    
    # Validation errors
    INVALID_REQUEST = "INVALID_REQUEST"
    INVALID_PARAMETER = "INVALID_PARAMETER"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    
    # Operation errors
    OPERATION_FAILED = "OPERATION_FAILED"
    OPERATION_TIMEOUT = "OPERATION_TIMEOUT"
    OPERATION_CANCELLED = "OPERATION_CANCELLED"
    
    # Resource errors
    NOT_FOUND = "NOT_FOUND"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    RESOURCE_BUSY = "RESOURCE_BUSY"
    
    # Internal errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"


class TaskResponse(ApiResponse[Dict[str, Any]]):
    """
    Specialized response for task operations.
    
    Commonly used for async task submission where the task_id is returned.
    """
    pass


class ListResponse(ApiResponse[List[Dict[str, Any]]]):
    """
    Specialized response for list operations.
    
    The data field contains a list of items.
    """
    pass


class StatusResponse(ApiResponse[Dict[str, Any]]):
    """
    Specialized response for status queries.
    
    The data field typically contains status information like:
    - state: str (e.g., "running", "idle", "error")
    - details: dict with additional status info
    """
    pass


def create_success_response(data: Any = None, message: str = None) -> Dict[str, Any]:
    """
    Create a simple success response dictionary.
    
    Use this for simple cases where you don't need the full ApiResponse type.
    
    Args:
        data: Response data
        message: Optional success message
    
    Returns:
        Dictionary with standardized success format
    """
    return {
        "status": "success",
        "data": data,
        "message": message,
        "error_code": None
    }


def create_error_response(message: str, error_code: str = None, data: Any = None) -> Dict[str, Any]:
    """
    Create a simple error response dictionary.
    
    Use this for simple cases where you don't need the full ApiResponse type.
    
    Args:
        message: Error message
        error_code: Machine-readable error code
        data: Optional additional error data
    
    Returns:
        Dictionary with standardized error format
    """
    return {
        "status": "error",
        "data": data,
        "message": message,
        "error_code": error_code
    }
