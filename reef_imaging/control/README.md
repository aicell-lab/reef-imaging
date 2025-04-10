# (Full Tutorial is not done yet)

# Orchestrator Simulation Tutorial

This tutorial will guide you through running the `orchestrator_simulation.py` script in local mode.

## Prerequisites

- Ensure you have Python installed on your system.
- Set up your environment variables:
  - `REEF_LOCAL_TOKEN`: Use this token for local operations.
  - `REEF_LOCAL_WORKSPACE`: Each token is associated with a single workspace. Make sure to set this in your `.env` file.


## Running the Script

1. Open a terminal and navigate to the directory containing `orchestrator_simulation.py`.
2. Run the script in local mode using the following command:

```
python orchestrator_simulation.py --local
```

This command will set the server URL to `http://reef.dyn.scilifelab.se:9527` and use the local token and workspace specified in your environment variables.

## Notes

- Ensure that your local server is running and accessible at the specified URL.
- The script will connect to the local server using the provided token and workspace settings.

for local token, use REEF_LOCAL_TOKEN

each token has single workspace, notic it when you registering services, and write it as REEF_LOCAL_WORKSPACE in .env