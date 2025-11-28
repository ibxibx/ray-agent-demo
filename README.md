# Ray Agent Demo

A demonstration project for learning distributed computing with Ray, showcasing basic agent (actor) patterns and asynchronous task processing.

## Overview

This project demonstrates:
- Creating Ray actors (agents) that maintain state
- Processing tasks asynchronously across distributed workers
- Collecting and aggregating results from multiple agents
- Basic patterns for building distributed systems with Ray

## Installation

```bash
# Clone the repository
git clone https://github.com/ibxibx/ray-agent-demo.git
cd ray-agent-demo

# Install in development mode
pip install -e .

# Or install requirements directly
pip install -r requirements.txt
```

## Quick Start

Run the basic agent example:

```python
from basic_agent import demo_basic_agent
demo_basic_agent()
```

Or from the command line:

```bash
python basic_agent.py
```

## Project Structure

```
ray-agent-demo/
├── basic_agent.py      # Basic Ray actor demonstration
├── requirements.txt    # Project dependencies
├── setup.py           # Package setup configuration
└── __init__.py        # Package initialization
```

## Examples

### Basic Agent Usage

```python
import ray
from basic_agent import BasicAgent

# Initialize Ray
ray.init()

# Create an agent
agent = BasicAgent.remote("agent-001")

# Process a task
result = ray.get(agent.process_task.remote("my_task"))

# Get agent status
status = ray.get(agent.get_status.remote())
```

## Features

- **State Management**: Agents maintain their own state across method calls
- **Asynchronous Processing**: Submit tasks without blocking and collect results later
- **Task Tracking**: Each agent tracks the number of tasks processed and stores results
- **Status Monitoring**: Query agent status at any time

## Next Steps

- Add multi-agent coordination examples
- Implement agent communication patterns
- Add error handling and resilience features
- Create performance benchmarks
- Build a distributed task queue example

## Requirements

- Python >= 3.8
- Ray >= 2.9.0
- NumPy >= 1.24.0
- Pandas >= 2.0.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
