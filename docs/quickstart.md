# Quick Start Guide

## Basic Usage

### 1. Import and Initialize

```python
import ray
from ray_agent_demo import BasicAgent

# Initialize Ray
ray.init()
```

### 2. Create an Agent

```python
# Create a basic agent
agent = BasicAgent.remote("my-agent")

# Process a task
result = ray.get(agent.process_task.remote("Hello, World!"))
```

### 3. Multi-Agent Coordination

```python
from ray_agent_demo import AgentCoordinator

# Create coordinator with 4 agents
coordinator = AgentCoordinator.remote(num_agents=4)

# Process multiple tasks
tasks = ["task1", "task2", "task3", "task4"]
results = ray.get(coordinator.process_all_tasks.remote(tasks))
```

## Next Steps

- Check out the [examples](../examples/) directory
- Read the [API documentation](api/)
- Try the advanced features in the [GUIDE](../GUIDE.md)
