# REEF Imaging Project Cursor Rules

## Project Overview
REEF Imaging is a platform for automated microscope control, image acquisition, data management, and analysis for reef biological experiments. The system integrates hardware control for microscopes, robotic arms, and incubators with the Hypha platform for cloud-based data management.

## Tech Stack & Dependencies
- **Python 3.11** (primary language)
- **Async/Await Pattern**: Heavy use of asyncio for concurrent operations
- **Hypha RPC**: Core platform for service communication and data management
- **WebRTC**: Real-time video streaming (aiortc, av libraries)
- **Docker & Docker Compose**: Containerized deployment
- **Redis**: Caching and message queuing
- **MinIO**: S3-compatible object storage
- **Traefik**: Reverse proxy and load balancer
- **Node.js/Express**: Frontend components
- **OpenCV**: Image processing
- **Zarr/OME-Zarr**: Scientific data formats
- **NumPy/Pandas**: Data manipulation

## Project Structure
```
reef_imaging/
├── control/                    # Hardware control modules
│   ├── dorna-control/         # Robotic arm control
│   ├── cytomat-control/       # Incubator control  
│   ├── squid-control/         # Microscope control
│   └── mirror-services/       # Cloud/local service proxies
├── hypha_tools/               # Hypha platform utilities
│   ├── artifact_manager/      # Data management tools
│   ├── chatbot/              # AI chat integration
│   └── automated_*_uploader.py # Data upload services
├── lab_live_stream/           # Camera streaming
├── tools/                     # Utility tools
│   ├── image_processing/      # Image processing utilities
├── orchestrator.py            # Main orchestration system
└── orchestrator_simulation.py # Testing/simulation mode
```

## Coding Standards & Patterns

### Python Code Style
- Use **type hints** for all function parameters and return values
- Follow **PEP 8** naming conventions (snake_case for functions/variables, PascalCase for classes)
- Use **async/await** for I/O operations, never blocking calls
- Implement **proper exception handling** with logging
- Use **dataclasses** or **Pydantic models** for structured data
- Add **docstrings** to all public functions and classes

### Async Programming Patterns
- Always use `asyncio.create_task()` for concurrent operations
- Implement proper connection pooling and resource cleanup
- Use context managers (`async with`) for resource management
- Handle `asyncio.CancelledError` appropriately
- Implement health checks and retry logic for services

### Service Architecture Patterns
- Use the **Mirror Service Pattern** for cloud/local service proxies
- Implement **task status tracking** for all operations
- Use **generic base classes** for common functionality (see `GenericMirrorService`)
- Implement **health checks** and **automatic reconnection**
- Log all service interactions with proper levels

### Logging Standards
- Use **structured logging** with timestamps and levels
- Implement **rotating file handlers** for production
- Use **correlation IDs** for tracing operations
- Log both to console and files
- Use appropriate log levels (DEBUG, INFO, WARNING, ERROR)

### Error Handling
- Implement **exponential backoff** for retries
- Use **circuit breaker pattern** for external services
- Handle **timeout scenarios** gracefully
- Provide **meaningful error messages** to users
- Log full stack traces for debugging

## Hardware Integration Rules

### Microscope Control
- Always check illumination state before operations
- Implement **proper focus management** (laser autofocus preferred)  
- Use **WebRTC** for real-time video streaming
- Handle **stage positioning** with safety checks
- Implement **well plate navigation** with coordinate validation

### Robotic Arm Integration
- Use **coordinate frame transformations** for positioning
- Implement **collision detection** and safety stops
- Handle **plate loading/unloading** sequences carefully
- Use **stress testing** for reliability validation
- Implement **graceful degradation** on hardware failures

### Incubator Management
- Track **sample locations** and environmental conditions
- Implement **slot management** and availability checking
- Handle **temperature and humidity** monitoring
- Use **barcode/RFID** tracking when available

## Data Management Rules

### Hypha Integration
- Use **artifact manager** for all file uploads
- Implement **gallery and dataset** organization
- Use **concurrent batch uploads** for performance
- Implement **resume capability** for interrupted transfers
- Track **upload progress** and provide user feedback

### Image Processing
- Use **OME-Zarr format** for microscopy data
- Implement **image stitching** with overlap handling
- Create **multi-resolution pyramids** for large images
- Use **channel-based uploads** for zarr files
- Implement **metadata preservation** throughout pipeline

### File Naming Conventions
- Use **ISO 8601 timestamps** in filenames
- Include **experiment metadata** in filenames
- Use **consistent channel naming** (e.g., BF_LED_matrix_full, Fluorescence_488_nm_Ex)
- Implement **collision-resistant** naming schemes

## Environment & Configuration

### Environment Variables
- Use **dotenv** for local development
- Store **sensitive credentials** in environment variables
- Use **different configurations** for development/production
- Implement **fallback defaults** where appropriate

### Required Environment Variables
```bash
# Cloud operation
REEF_WORKSPACE_TOKEN=<token>
SQUID_WORKSPACE_TOKEN=<token>

# Local development  
REEF_LOCAL_TOKEN=<token>
REEF_LOCAL_WORKSPACE=<workspace>

# Infrastructure
REDIS_PASSWORD=<password>
JWT_SECRET=<secret>
S3_ACCESS_KEY=<key>
S3_SECRET_KEY=<secret>
```

## Docker & Deployment Rules

### Container Architecture
- Use **multi-stage builds** for optimization
- Implement **health checks** for all services
- Use **named volumes** for persistent data
- Configure **proper networking** between services
- Implement **graceful shutdown** handling

### Service Dependencies
- Define **proper dependency order** in docker-compose
- Use **health check conditions** for startup sequencing  
- Implement **retry logic** for service connections
- Handle **service discovery** and registration

## Testing Guidelines

### Unit Testing
- Write tests for all **public interfaces**
- Use **asyncio test fixtures** for async code
- Mock **external service dependencies**
- Test **error conditions** and edge cases
- Implement **integration tests** for workflows

### Simulation Mode
- Use `orchestrator_simulation.py` for **safe testing**
- Implement **mock hardware responses**
- Test **timing and scheduling** logic
- Validate **error recovery** scenarios

## Security Considerations

### Authentication & Authorization
- Use **JWT tokens** for service authentication
- Implement **role-based access control** where needed
- Secure **API endpoints** with proper validation
- Use **HTTPS/WSS** for all external communications

### Data Security
- Encrypt **sensitive data** at rest and in transit
- Implement **access logging** for audit trails
- Use **secure credential management**
- Validate **all user inputs** before processing

## Performance Guidelines

### Async Optimization
- Use **connection pooling** for database/service connections
- Implement **batching** for bulk operations
- Use **streaming** for large data transfers
- Avoid **blocking calls** in async contexts

### Resource Management
- Implement **proper cleanup** of resources
- Use **memory-efficient** data structures for large datasets
- Monitor **resource usage** and implement limits
- Use **lazy loading** for expensive operations

## Monitoring & Observability

### Logging Strategy
- Use **structured logging** with consistent formats
- Implement **log aggregation** for distributed systems
- Use **correlation IDs** for request tracing
- Monitor **error rates** and response times

### Health Monitoring
- Implement **health check endpoints** for all services
- Monitor **hardware status** and connectivity
- Track **experiment progress** and completion rates
- Alert on **critical failures** or anomalies

## AI Integration Rules
- Implement **chatbot integration** for user assistance through Hypha platform
- Use **GPU acceleration** when available for image processing tasks
- Cache **processed results** for repeated operations to improve performance

## File and Code Organization
- Keep **configuration files** in JSON format with validation
- Use **relative imports** within the package
- Implement **plugin architecture** for extensibility
- Maintain **clear separation** between hardware control and business logic

## Common Patterns to Follow
1. **Service Registration Pattern**: All services register with Hypha using consistent metadata
2. **Task Status Pattern**: Track operation status (not_started, started, finished, failed)
3. **Health Check Pattern**: Implement periodic health checks with automatic recovery
4. **Mirror Service Pattern**: Proxy between cloud and local services with fallback
5. **Retry Pattern**: Exponential backoff with circuit breaker for external calls
6. **Cleanup Pattern**: Always implement proper resource cleanup in finally blocks

## When Working with This Codebase
- Always run services in **simulation mode** first before hardware
- Check **service health** before attempting operations
- Use **proper error handling** for all hardware interactions
- Implement **graceful degradation** when services are unavailable
- Follow the **async patterns** established in the orchestrator
- Use the **established logging** and **monitoring** patterns
- Test with **small datasets** before processing large experiments 