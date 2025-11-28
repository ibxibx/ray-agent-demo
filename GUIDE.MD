# Ray Agent Demo - Developer Guide

## Overview

This project has been expanded with comprehensive examples demonstrating various Ray agent patterns and distributed computing concepts. Here's what's included:

## Project Structure

```
ray-agent-demo/
├── basic_agent.py              # Original basic agent example
├── multi_agent_coordinator.py  # NEW: Multi-agent coordination patterns
├── agent_communication.py      # NEW: Inter-agent communication
├── distributed_task_queue.py   # NEW: Production-ready task queue
├── improved_agent.py          # NEW: Enhanced agent with advanced features
├── test_agents.py            # NEW: Comprehensive test suite
├── run_all_demos.py          # NEW: Run all demonstrations
├── requirements.txt          # Updated with new dependencies
├── README.md                 # Project overview
└── GUIDE.md                 # This file
```

## New Components

### 1. **multi_agent_coordinator.py**
Demonstrates coordinating multiple agents to process tasks in parallel.

**Features:**
- Round-robin task distribution
- Load-balanced task assignment
- Workload monitoring and statistics

**Usage:**
```python
from multi_agent_coordinator import AgentCoordinator

coordinator = AgentCoordinator.remote(num_agents=4)
results = ray.get(coordinator.process_all_tasks.remote(tasks))
```

### 2. **agent_communication.py**
Shows different patterns for agents to communicate with each other.

**Features:**
- Direct agent-to-agent messaging
- Broadcast messaging
- Publish-subscribe pattern with message router
- Message types (TASK, RESULT, STATUS, PING/PONG)

**Usage:**
```python
from agent_communication import CommunicatingAgent, MessageType

agent1 = CommunicatingAgent.remote("agent-1")
agent2 = CommunicatingAgent.remote("agent-2")
agent1.send_message.remote("agent-2", MessageType.TASK, "compute_sum(1,2)")
```

### 3. **distributed_task_queue.py**
A production-ready distributed task queue implementation.

**Features:**
- Priority-based task scheduling
- Task retry logic with configurable limits
- Worker pool management
- Real-time queue monitoring
- Support for different task types (compute, data_transform, io_task)

**Usage:**
```python
from distributed_task_queue import TaskQueue, TaskWorker, Task, TaskPriority

queue = TaskQueue.remote("main_queue")
task = Task(
    task_id="task_001",
    task_type="compute",
    payload={"operation": "sum", "data": [1, 2, 3]},
    priority=TaskPriority.HIGH
)
queue.submit_task.remote(task)
```

### 4. **improved_agent.py**
Enhanced version of the basic agent with production features.

**Features:**
- Async task processing with asyncio
- Concurrent task limits
- Comprehensive error handling
- Metrics collection and monitoring
- State persistence to disk
- Batch processing capabilities

**Usage:**
```python
from improved_agent import ImprovedAgent

agent = ImprovedAgent.remote("agent-001", max_concurrent_tasks=5)
# Async processing
result = await agent.process_task_async(task)
# Batch processing
results = agent.process_batch(tasks)
```

### 5. **test_agents.py**
Comprehensive test suite using pytest.

**Features:**
- Unit tests for all agent types
- Integration tests for communication patterns
- Performance benchmarks
- Error handling verification

**Usage:**
```bash
pytest test_agents.py -v
# Run specific test class
pytest test_agents.py::TestBasicAgent -v
# Run with performance output
pytest test_agents.py::TestPerformance -v -s
```

### 6. **run_all_demos.py**
Convenience script to run all demonstrations sequentially.

**Usage:**
```bash
python run_all_demos.py
```

## Getting Started

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Run all demos:**
```bash
python run_all_demos.py
```

3. **Run tests:**
```bash
pytest test_agents.py -v
```

4. **Try individual examples:**
```bash
python multi_agent_coordinator.py
python agent_communication.py
python distributed_task_queue.py
python improved_agent.py
```

## Key Concepts Demonstrated

### 1. **Ray Actors (Agents)**
- Stateful workers that can process tasks
- Maintain internal state across method calls
- Can be distributed across multiple machines

### 2. **Asynchronous Processing**
- Submit tasks without blocking
- Process multiple tasks concurrently
- Collect results when ready

### 3. **Agent Coordination**
- Distribute work across multiple agents
- Balance load for optimal performance
- Monitor and manage agent pools

### 4. **Communication Patterns**
- Direct messaging between agents
- Broadcast to multiple recipients
- Publish-subscribe for decoupled communication

### 5. **Production Features**
- Error handling and retry logic
- Metrics and monitoring
- State persistence
- Priority-based scheduling

## Best Practices

1. **Resource Management:**
   - Set appropriate concurrent task limits
   - Monitor agent workloads
   - Use load balancing for even distribution

2. **Error Handling:**
   - Implement retry logic for transient failures
   - Log errors for debugging
   - Provide graceful degradation

3. **Monitoring:**
   - Collect metrics on task processing
   - Monitor agent health and status
   - Track queue lengths and processing times

4. **Testing:**
   - Write unit tests for agent logic
   - Test error conditions
   - Benchmark performance under load

## Next Steps

1. **Customize for Your Use Case:**
   - Modify task processors for your specific needs
   - Add new message types or task types
   - Implement custom scheduling algorithms

2. **Scale Up:**
   - Deploy on a Ray cluster
   - Add more agents dynamically
   - Implement auto-scaling based on load

3. **Add Persistence:**
   - Store results in a database
   - Implement checkpoint/restart
   - Add task history tracking

4. **Integrate with Your System:**
   - Create REST API endpoints
   - Add webhook notifications
   - Integrate with monitoring systems

## Troubleshooting

**Ray not initialized:**
```python
ray.init(ignore_reinit_error=True)
```

**Port conflicts:**
```python
ray.init(dashboard_port=8266)  # Use different port
```

**Memory issues:**
```python
ray.init(object_store_memory=2_000_000_000)  # 2GB
```

**Debugging:**
```python
ray.init(local_mode=True)  # Run synchronously for debugging
```

## Additional Resources

- [Ray Documentation](https://docs.ray.io/)
- [Ray Design Patterns](https://docs.ray.io/en/latest/ray-design-patterns/index.html)
- [Ray Best Practices](https://docs.ray.io/en/latest/ray-core/best-practices.html)

## Contributing

Feel free to extend these examples and add new patterns! Some ideas:
- Fault-tolerant agents with automatic restart
- Agent hierarchies (supervisor-worker patterns)
- Dynamic agent scaling based on load
- Integration with Ray Serve for HTTP endpoints
- Using Ray Tune for hyperparameter optimization
