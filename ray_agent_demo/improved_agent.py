"""
Improved Ray Agent Example
Enhanced version with error handling, async methods, state persistence, and monitoring.
"""

import ray
import time
import asyncio
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from collections import deque
import logging


class TaskMetrics:
    """Optimized metrics for a processed task using __slots__ for memory efficiency."""
    __slots__ = ['task_id', 'start_time', 'end_time', 'success', 'error_message']
    
    def __init__(self, task_id: str, start_time: float, end_time: float = 0, 
                 success: bool = False, error_message: Optional[str] = None):
        self.task_id = task_id
        self.start_time = start_time
        self.end_time = end_time
        self.success = success
        self.error_message = error_message
    
    @property
    def duration(self) -> float:
        """Calculate task duration in seconds."""
        return self.end_time - self.start_time
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            'task_id': self.task_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'success': self.success,
            'error_message': self.error_message,
            'duration': self.duration
        }


@ray.remote
class ImprovedAgent:
    """
    Optimized Ray agent with enhanced performance:
    - Efficient async task processing
    - Lightweight error handling
    - In-memory metrics with circular buffer
    - Minimal overhead operations
    """
    
    def __init__(self, agent_id: str, max_concurrent_tasks: int = 5, max_results_history: int = 1000):
        self.agent_id = agent_id
        self.max_concurrent_tasks = max_concurrent_tasks
        self.tasks_processed = 0
        self.tasks_failed = 0
        
        # Use deque for O(1) append and automatic size limiting
        self.results = deque(maxlen=max_results_history)
        self.metrics = deque(maxlen=max_results_history)
        
        # Use set for O(1) lookups and additions
        self.current_task_ids = set()
        
        # Pre-compile operations for faster lookup
        self._operations = {
            "sum": self._op_sum,
            "multiply": self._op_multiply,
            "filter": self._op_filter,
            "transform": self._op_transform
        }
        
    
    # Optimized operation methods
    @staticmethod
    def _op_sum(data: List) -> Any:
        return sum(data)
    
    @staticmethod
    def _op_multiply(data: List) -> Any:
        result = 1
        for num in data:
            result *= num
        return result
    
    @staticmethod
    def _op_filter(data: List, condition=None) -> List:
        if condition is None:
            return [x for x in data if x > 0]
        return [x for x in data if condition(x)]
    
    @staticmethod  
    def _op_transform(data: List, transform_fn=None) -> List:
        if transform_fn is None:
            return [x * 2 for x in data]
        return [transform_fn(x) for x in data]
    
    async def process_task_async(self, task: Any, task_id: Optional[str] = None) -> Dict:
        """
        Process a task asynchronously with minimal overhead.
        """
        if task_id is None:
            task_id = f"task_{self.tasks_processed + 1}"
        
        start_time = time.perf_counter()  # More precise than time.time()
        
        try:
            # Fast capacity check
            if len(self.current_task_ids) >= self.max_concurrent_tasks:
                raise RuntimeError(f"Agent at max capacity ({self.max_concurrent_tasks} tasks)")
            
            self.current_task_ids.add(task_id)
            
            # Minimal async delay for cooperative multitasking
            await asyncio.sleep(0)
            
            # Fast path for dict operations
            if isinstance(task, dict) and "operation" in task:
                op_func = self._operations.get(task["operation"])
                if op_func:
                    result = op_func(
                        task.get("data", []),
                        *([task.get("condition")] if task["operation"] == "filter" else 
                          [task.get("transform")] if task["operation"] == "transform" else [])
                    )
                else:
                    raise ValueError(f"Unknown operation: {task['operation']}")
            else:
                result = f"Processed: {task}"
            
            self.tasks_processed += 1
            
            task_result = {
                "agent_id": self.agent_id,
                "task_id": task_id,
                "result": result,
                "success": True,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.tasks_failed += 1
            
            task_result = {
                "agent_id": self.agent_id,
                "task_id": task_id,
                "error": str(e),
                "success": False,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        finally:
            end_time = time.perf_counter()
            
            # Lightweight metrics
            metrics = TaskMetrics(
                task_id=task_id,
                start_time=start_time,
                end_time=end_time,
                success=task_result["success"],
                error_message=task_result.get("error")
            )
            
            self.current_task_ids.discard(task_id)
            self.metrics.append(metrics)
            self.results.append(task_result)
        
        return task_result

    
    def process_task(self, task: Any, task_id: Optional[str] = None) -> Dict:
        """Synchronous wrapper for async task processing."""
        return asyncio.run(self.process_task_async(task, task_id))
    
    async def process_batch_async(self, tasks: List[Any]) -> List[Dict]:
        """
        Process multiple tasks concurrently with optimal batching.
        """
        # Create task list with pre-allocated IDs
        task_list = [(task, f"batch_{self.tasks_processed + i + 1}") 
                     for i, task in enumerate(tasks)]
        
        # Process concurrently with gather
        results = await asyncio.gather(
            *[self.process_task_async(task, tid) for task, tid in task_list],
            return_exceptions=False
        )
        
        return results
    
    def process_batch(self, tasks: List[Any]) -> List[Dict]:
        """Synchronous wrapper for batch processing."""
        return asyncio.run(self.process_batch_async(tasks))
    
    def get_status(self) -> Dict:
        """Get agent status with cached calculations."""
        total_tasks = self.tasks_processed + self.tasks_failed
        
        if total_tasks > 0:
            success_rate = self.tasks_processed / total_tasks
            
            # Fast average calculation for recent metrics
            recent_durations = [m.duration for m in list(self.metrics)[-100:] 
                               if m.success]
            avg_duration = (sum(recent_durations) / len(recent_durations) 
                           if recent_durations else 0)
        else:
            success_rate = 0
            avg_duration = 0
        
        return {
            "agent_id": self.agent_id,
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed,
            "success_rate": round(success_rate, 3),
            "current_tasks": len(self.current_task_ids),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "average_duration": round(avg_duration, 3),
            "is_alive": True
        }
    
    def get_metrics(self) -> List[Dict]:
        """Get recent task metrics."""
        return [m.to_dict() for m in self.metrics]
    
    def clear_history(self):
        """Clear task history and metrics."""
        self.results.clear()
        self.metrics.clear()
        return f"History cleared for {self.agent_id}"


@ray.remote
class AgentMonitor:
    """Optimized monitor for multiple agents with minimal overhead."""
    
    def __init__(self):
        self.agents = {}  # agent_id -> agent_ref
        self.monitoring_interval = 5.0
        self.is_monitoring = False
        self.metrics_buffer = deque(maxlen=100)  # Keep only recent metrics
    
    def register_agent(self, agent_id: str, agent_ref: Any):
        """Register an agent for monitoring."""
        self.agents[agent_id] = agent_ref
        return f"Registered agent: {agent_id}"
    
    async def monitor_agents_async(self, duration: float = 30.0):
        """Monitor agents with efficient batch collection."""
        self.is_monitoring = True
        start_time = time.perf_counter()
        
        while self.is_monitoring and (time.perf_counter() - start_time) < duration:
            # Batch collect metrics
            metrics_futures = {
                agent_id: agent_ref.get_status.remote()
                for agent_id, agent_ref in self.agents.items()
            }
            
            # Wait for all metrics
            metrics = {}
            for agent_id, future in metrics_futures.items():
                try:
                    metrics[agent_id] = await future
                except Exception as e:
                    metrics[agent_id] = {"error": str(e)}
            
            self.metrics_buffer.append({
                "timestamp": datetime.utcnow().isoformat(),
                "metrics": metrics
            })
            
            await asyncio.sleep(self.monitoring_interval)
    
    def get_summary(self) -> Dict:
        """Get efficient monitoring summary."""
        if not self.metrics_buffer:
            return {"error": "No metrics collected"}
        
        latest_metrics = self.metrics_buffer[-1]["metrics"]
        
        # Fast aggregation
        total_processed = sum(
            m.get("tasks_processed", 0) 
            for m in latest_metrics.values() 
            if "error" not in m
        )
        total_failed = sum(
            m.get("tasks_failed", 0) 
            for m in latest_metrics.values() 
            if "error" not in m
        )
        
        return {
            "num_agents": len(self.agents),
            "total_tasks_processed": total_processed,
            "total_tasks_failed": total_failed,
            "monitoring_duration": len(self.metrics_buffer) * self.monitoring_interval,
            "last_update": self.metrics_buffer[-1]["timestamp"]
        }


def demo_improved_agent():
    """Demonstrate optimized agent features."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Optimized Agent Demo ===\n")
    
    # Create an optimized agent
    agent = ImprovedAgent.remote("optimized-001", max_concurrent_tasks=5)
    
    # Test 1: Basic task processing
    print("1. Basic task processing:")
    result = ray.get(agent.process_task.remote({"operation": "sum", "data": [1, 2, 3, 4, 5]}))
    print(f"   Result: {result['result']}")
    
    # Test 2: Batch processing with various operations
    print("\n2. Batch processing:")
    batch_tasks = [
        {"operation": "sum", "data": list(range(1, 11))},
        {"operation": "multiply", "data": [2, 3, 4, 5]},
        {"operation": "filter", "data": list(range(-5, 6))},
        {"operation": "transform", "data": [1, 2, 3, 4]},
        "simple_string_task"
    ]
    
    start_time = time.perf_counter()
    results = ray.get(agent.process_batch.remote(batch_tasks))
    batch_time = time.perf_counter() - start_time
    
    for i, res in enumerate(results):
        if res.get("success", False):
            print(f"   Task {i+1}: Success - {res.get('result')}")
        else:
            print(f"   Task {i+1}: Failed - {res.get('error')}")
    print(f"   Batch processing time: {batch_time:.3f}s")
    
    # Test 3: Performance metrics
    print("\n3. Performance test:")
    num_tasks = 100
    test_tasks = [{"operation": "sum", "data": list(range(i, i+10))} 
                  for i in range(num_tasks)]
    
    start_time = time.perf_counter()
    results = ray.get(agent.process_batch.remote(test_tasks))
    total_time = time.perf_counter() - start_time
    
    successful = sum(1 for r in results if r.get("success", False))
    print(f"   Processed {successful}/{num_tasks} tasks in {total_time:.3f}s")
    print(f"   Throughput: {successful/total_time:.1f} tasks/second")
    
    # Test 4: Agent status
    print("\n4. Agent status:")
    status = ray.get(agent.get_status.remote())
    for key, value in status.items():
        print(f"   {key}: {value}")
    
    ray.shutdown()


def demo_monitoring():
    """Demonstrate optimized agent monitoring."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Optimized Monitoring Demo ===\n")
    
    # Create multiple agents
    num_agents = 4
    agents = []
    monitor = AgentMonitor.remote()
    
    print(f"Creating {num_agents} agents...")
    for i in range(num_agents):
        agent_id = f"worker-{i:03d}"
        agent = ImprovedAgent.remote(agent_id, max_concurrent_tasks=10)
        agents.append(agent)
        ray.get(monitor.register_agent.remote(agent_id, agent))
    
    # Start monitoring in background
    monitor_task = monitor.monitor_agents_async.remote(duration=10.0)
    
    # Simulate high-throughput workload
    print("Processing high-volume tasks...")
    total_tasks = 200
    
    # Distribute tasks efficiently
    futures = []
    for i in range(total_tasks):
        agent = agents[i % num_agents]
        task = {
            "operation": ["sum", "multiply", "filter", "transform"][i % 4],
            "data": list(range(i % 10 + 1))
        }
        futures.append(agent.process_task.remote(task))
    
    # Wait for all tasks
    start_time = time.perf_counter()
    results = ray.get(futures)
    processing_time = time.perf_counter() - start_time
    
    successful = sum(1 for r in results if r["success"])
    print(f"\nCompleted {successful}/{total_tasks} tasks in {processing_time:.3f}s")
    print(f"Overall throughput: {successful/processing_time:.1f} tasks/second")
    
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
