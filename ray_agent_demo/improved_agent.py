"""
Improved Ray Agent Example
Enhanced version with error handling, async methods, state persistence, and monitoring.
"""

import ray
import time
import json
import asyncio
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, asdict
import logging
from pathlib import Path


@dataclass
class TaskMetrics:
    """Metrics for a processed task."""
    task_id: str
    start_time: float
    end_time: float
    success: bool
    error_message: Optional[str] = None
    
    @property
    def duration(self) -> float:
        """Calculate task duration in seconds."""
        return self.end_time - self.start_time


@ray.remote
class ImprovedAgent:
    """
    Enhanced Ray agent with advanced features:
    - Async task processing
    - Error handling and retry logic
    - State persistence
    - Metrics collection
    - Resource monitoring
    """
    
    def __init__(self, agent_id: str, max_concurrent_tasks: int = 5):
        self.agent_id = agent_id
        self.max_concurrent_tasks = max_concurrent_tasks
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.results = []
        self.metrics = []
        self.current_tasks = {}  # task_id -> task_info
        self.state_file = f"/tmp/agent_{agent_id}_state.json"
        
        # Setup logging
        self.logger = logging.getLogger(f"Agent-{agent_id}")
        self.logger.setLevel(logging.INFO)
        
        # Load previous state if exists
        self._load_state()
    
    async def process_task_async(self, task: Any, task_id: Optional[str] = None) -> Dict:
        """
        Process a task asynchronously with error handling.
        
        Args:
            task: The task to process
            task_id: Optional task identifier
            
        Returns:
            Dict containing the result and metadata
        """
        if task_id is None:
            task_id = f"task_{self.tasks_processed + 1}"
        
        start_time = time.time()
        metrics = TaskMetrics(
            task_id=task_id,
            start_time=start_time,
            end_time=0,
            success=False
        )
        
        try:
            # Check concurrent task limit
            if len(self.current_tasks) >= self.max_concurrent_tasks:
                raise RuntimeError(f"Agent at max capacity ({self.max_concurrent_tasks} tasks)")
            
            # Register task
            self.current_tasks[task_id] = {
                "task": task,
                "start_time": start_time,
                "status": "processing"
            }
            
            # Simulate async processing
            await asyncio.sleep(0.1)
            
            # Process based on task type
            if isinstance(task, dict) and "operation" in task:
                result = await self._process_operation(task)
            else:
                result = f"Processed: {task}"
            
            # Success
            self.tasks_processed += 1
            metrics.success = True
            
            task_result = {
                "agent_id": self.agent_id,
                "task_id": task_id,
                "task": task,
                "result": result,
                "success": True,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            # Handle errors
            self.tasks_failed += 1
            metrics.success = False
            metrics.error_message = str(e)
            
            self.logger.error(f"Task {task_id} failed: {e}")
            
            task_result = {
                "agent_id": self.agent_id,
                "task_id": task_id,
                "task": task,
                "error": str(e),
                "success": False,
                "timestamp": datetime.now().isoformat()
            }
        
        finally:
            # Cleanup
            end_time = time.time()
            metrics.end_time = end_time
            
            if task_id in self.current_tasks:
                del self.current_tasks[task_id]
            
            # Store metrics
            self.metrics.append(metrics)
            self.results.append(task_result)
            
            # Persist state periodically
            if self.tasks_processed % 10 == 0:
                self._save_state()
        
        return task_result
    
    async def _process_operation(self, task: Dict) -> Any:
        """Process different types of operations."""
        operation = task.get("operation")
        data = task.get("data", [])
        
        if operation == "sum":
            return sum(data)
        elif operation == "multiply":
            result = 1
            for num in data:
                result *= num
            return result
        elif operation == "filter":
            condition = task.get("condition", lambda x: x > 0)
            return [x for x in data if condition(x)]
        elif operation == "transform":
            transform_fn = task.get("transform", lambda x: x * 2)
            return [transform_fn(x) for x in data]
        else:
            raise ValueError(f"Unknown operation: {operation}")
    
    def process_task(self, task: Any, task_id: Optional[str] = None) -> Dict:
        """Synchronous wrapper for async task processing."""
        return asyncio.run(self.process_task_async(task, task_id))
    
    async def process_batch_async(self, tasks: List[Any]) -> List[Dict]:
        """
        Process multiple tasks concurrently.
        
        Args:
            tasks: List of tasks to process
            
        Returns:
            List of results
        """
        # Create tasks with IDs
        task_list = []
        for i, task in enumerate(tasks):
            task_id = f"batch_{self.tasks_processed + i + 1}"
            task_list.append((task, task_id))
        
        # Process concurrently
        results = await asyncio.gather(
            *[self.process_task_async(task, tid) for task, tid in task_list],
            return_exceptions=True
        )
        
        # Handle any exceptions
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({
                    "error": str(result),
                    "success": False
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    def process_batch(self, tasks: List[Any]) -> List[Dict]:
        """Synchronous wrapper for batch processing."""
        return asyncio.run(self.process_batch_async(tasks))
    
    def get_status(self) -> Dict:
        """Get comprehensive agent status."""
        success_rate = (
            self.tasks_processed / (self.tasks_processed + self.tasks_failed)
            if (self.tasks_processed + self.tasks_failed) > 0
            else 0
        )
        
        # Calculate average processing time
        successful_metrics = [m for m in self.metrics if m.success]
        avg_duration = (
            sum(m.duration for m in successful_metrics) / len(successful_metrics)
            if successful_metrics
            else 0
        )
        
        return {
            "agent_id": self.agent_id,
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed,
            "success_rate": success_rate,
            "current_tasks": len(self.current_tasks),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "average_duration": avg_duration,
            "is_alive": True
        }
    
    def get_metrics(self) -> List[Dict]:
        """Get detailed task metrics."""
        return [asdict(m) for m in self.metrics]
    
    def clear_history(self):
        """Clear task history and metrics."""
        self.results.clear()
        self.metrics.clear()
        self._save_state()
        return f"History cleared for {self.agent_id}"
    
    def _save_state(self):
        """Persist agent state to disk."""
        state = {
            "agent_id": self.agent_id,
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    def _load_state(self):
        """Load agent state from disk."""
        try:
            if Path(self.state_file).exists():
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.tasks_processed = state.get("tasks_processed", 0)
                    self.tasks_failed = state.get("tasks_failed", 0)
                    self.logger.info(f"Loaded state: {state}")
        except Exception as e:
            self.logger.error(f"Failed to load state: {e}")


@ray.remote
class AgentMonitor:
    """Monitor multiple agents and collect aggregated metrics."""
    
    def __init__(self):
        self.agents = {}  # agent_id -> agent_ref
        self.monitoring_interval = 5.0  # seconds
        self.is_monitoring = False
        self.metrics_history = []
    
    def register_agent(self, agent_id: str, agent_ref: Any):
        """Register an agent for monitoring."""
        self.agents[agent_id] = agent_ref
        return f"Registered agent: {agent_id}"
    
    async def monitor_agents_async(self, duration: float = 30.0):
        """Monitor agents for a specified duration."""
        self.is_monitoring = True
        start_time = time.time()
        
        while self.is_monitoring and (time.time() - start_time) < duration:
            # Collect metrics from all agents
            metrics = {}
            for agent_id, agent_ref in self.agents.items():
                try:
                    status = await agent_ref.get_status.remote()
                    metrics[agent_id] = status
                except Exception as e:
                    metrics[agent_id] = {"error": str(e)}
            
            self.metrics_history.append({
                "timestamp": datetime.now().isoformat(),
                "metrics": metrics
            })
            
            await asyncio.sleep(self.monitoring_interval)
    
    def get_summary(self) -> Dict:
        """Get monitoring summary."""
        if not self.metrics_history:
            return {"error": "No metrics collected"}
        
        # Aggregate metrics
        total_processed = 0
        total_failed = 0
        
        latest_metrics = self.metrics_history[-1]["metrics"]
        for agent_metrics in latest_metrics.values():
            if "error" not in agent_metrics:
                total_processed += agent_metrics.get("tasks_processed", 0)
                total_failed += agent_metrics.get("tasks_failed", 0)
        
        return {
            "num_agents": len(self.agents),
            "total_tasks_processed": total_processed,
            "total_tasks_failed": total_failed,
            "monitoring_duration": len(self.metrics_history) * self.monitoring_interval,
            "last_update": self.metrics_history[-1]["timestamp"]
        }


def demo_improved_agent():
    """Demonstrate improved agent features."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Improved Agent Demo ===\n")
    
    # Create an improved agent
    agent = ImprovedAgent.remote("improved-001", max_concurrent_tasks=3)
    
    # Test 1: Basic task processing
    print("1. Basic task processing:")
    result = ray.get(agent.process_task.remote({"operation": "sum", "data": [1, 2, 3, 4, 5]}))
    print(f"   Result: {result['result']}")
    
    # Test 2: Batch processing
    print("\n2. Batch processing:")
    batch_tasks = [
        {"operation": "sum", "data": [1, 2, 3]},
        {"operation": "multiply", "data": [2, 3, 4]},
        {"operation": "unknown", "data": [1, 2, 3]},  # This will fail
        "simple_task"
    ]
    
    results = ray.get(agent.process_batch.remote(batch_tasks))
    for i, res in enumerate(results):
        if res.get("success", False):
            print(f"   Task {i+1}: Success - {res.get('result')}")
        else:
            print(f"   Task {i+1}: Failed - {res.get('error')}")
    
    # Test 3: Get metrics
    print("\n3. Agent metrics:")
    metrics = ray.get(agent.get_metrics.remote())
    print(f"   Total metrics collected: {len(metrics)}")
    if metrics:
        avg_duration = sum(m["duration"] for m in metrics) / len(metrics)
        print(f"   Average task duration: {avg_duration:.3f}s")
    
    # Test 4: Status check
    print("\n4. Agent status:")
    status = ray.get(agent.get_status.remote())
    for key, value in status.items():
        print(f"   {key}: {value}")
    
    ray.shutdown()


def demo_monitoring():
    """Demonstrate agent monitoring capabilities."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Agent Monitoring Demo ===\n")
    
    # Create multiple agents
    num_agents = 3
    agents = []
    monitor = AgentMonitor.remote()
    
    print(f"Creating {num_agents} agents...")
    for i in range(num_agents):
        agent_id = f"monitored-{i:03d}"
        agent = ImprovedAgent.remote(agent_id)
        agents.append(agent)
        ray.get(monitor.register_agent.remote(agent_id, agent))
    
    # Start monitoring in background
    monitor_task = monitor.monitor_agents_async.remote(duration=20.0)
    
    # Simulate workload
    print("Processing tasks...")
    for i in range(10):
        # Distribute tasks to agents
        agent = agents[i % num_agents]
        task = {
            "operation": "sum" if i % 2 == 0 else "multiply",
            "data": list(range(1, i+2))
        }
        agent.process_task.remote(task)
        time.sleep(0.5)
    
    # Wait for monitoring to complete
    ray.get(monitor_task)
    
    # Get monitoring summary
    summary = ray.get(monitor.get_summary.remote())
    print("\nMonitoring Summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
    
    ray.shutdown()


if __name__ == "__main__":
    demo_improved_agent()
    print("\n" + "="*50 + "\n")
    demo_monitoring()
