"""
Distributed Task Queue Example
Implements a distributed task queue system using Ray agents.
"""

import ray
import time
from typing import Any, Dict, List, Optional
from enum import Enum
from datetime import datetime
from collections import deque
import heapq


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class TaskPriority(Enum):
    """Task priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Task:
    """Represents a task in the queue."""
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3
    timeout: float = 300.0  # 5 minutes default
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: str = None
    worker_id: str = None
    start_time: float = None
    end_time: float = None
    retries: int = 0
    
    @property
    def execution_time(self) -> float:
        """Calculate execution time in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


@ray.remote
class TaskQueue:
    """
    Distributed task queue that manages task distribution to workers.
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.pending_tasks = []  # Priority queue
        self.running_tasks = {}  # task_id -> (task, worker_id, start_time)
        self.completed_tasks = {}  # task_id -> TaskResult
        self.failed_tasks = {}  # task_id -> TaskResult
        self.task_retries = {}  # task_id -> retry_count
        
    def submit_task(self, task: Task) -> str:
        """Submit a task to the queue."""
        self.pending_tasks.append(task)
        # Sort by priority (higher first) and then by creation time (older first)
        self.pending_tasks.sort(
            key=lambda t: (-t.priority.value, t.created_at)
        )
        return task.task_id
    
    def get_next_task(self, worker_id: str) -> Optional[Task]:
        """Get the next available task for a worker."""
        if not self.pending_tasks:
            return None
        
        task = self.pending_tasks.pop(0)
        self.running_tasks[task.task_id] = (task, worker_id, time.time())
        return task
    
    def complete_task(self, result: TaskResult):
        """Mark a task as completed."""
        task_id = result.task_id
        if task_id in self.running_tasks:
            del self.running_tasks[task_id]
        
        if result.status == TaskStatus.COMPLETED:
            self.completed_tasks[task_id] = result
        elif result.status == TaskStatus.FAILED:
            # Check if we should retry
            task, _, _ = self.running_tasks.get(task_id, (None, None, None))
            if task:
                retry_count = self.task_retries.get(task_id, 0)
                if retry_count < task.max_retries:
                    self.task_retries[task_id] = retry_count + 1
                    task.priority = TaskPriority.HIGH  # Boost priority for retries
                    self.submit_task(task)
                    result.status = TaskStatus.RETRYING
                else:
                    self.failed_tasks[task_id] = result
            else:
                self.failed_tasks[task_id] = result
    
    def get_queue_status(self) -> Dict:
        """Get current queue status."""
        return {
            "queue_name": self.name,
            "pending": len(self.pending_tasks),
            "running": len(self.running_tasks),
            "completed": len(self.completed_tasks),
            "failed": len(self.failed_tasks),
            "total_processed": len(self.completed_tasks) + len(self.failed_tasks)
        }
    
    def get_task_status(self, task_id: str) -> Optional[str]:
        """Get status of a specific task."""
        if task_id in [t.task_id for t in self.pending_tasks]:
            return TaskStatus.PENDING.value
        elif task_id in self.running_tasks:
            return TaskStatus.RUNNING.value
        elif task_id in self.completed_tasks:
            return TaskStatus.COMPLETED.value
        elif task_id in self.failed_tasks:
            return TaskStatus.FAILED.value
        return None


@ray.remote
class TaskWorker:
    """
    Worker that processes tasks from the queue.
    """
    
    def __init__(self, worker_id: str, task_queue: TaskQueue):
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.tasks_processed = 0
        self.total_execution_time = 0.0
        self.is_running = True
        
        # Task processors for different task types
        self.task_processors = {
            "compute": self._process_compute_task,
            "data_transform": self._process_data_transform_task,
            "io_task": self._process_io_task,
        }
    
    def start(self):
        """Start processing tasks from the queue."""
        while self.is_running:
            # Get next task
            task = ray.get(self.task_queue.get_next_task.remote(self.worker_id))
            
            if task is None:
                # No tasks available, wait a bit
                time.sleep(0.1)
                continue
            
            # Process the task
            result = self._process_task(task)
            
            # Submit result back to queue
            ray.get(self.task_queue.complete_task.remote(result))
            
            self.tasks_processed += 1
            self.total_execution_time += result.execution_time
    
    def stop(self):
        """Stop the worker."""
        self.is_running = False
    
    def _process_task(self, task: Task) -> TaskResult:
        """Process a single task."""
        start_time = time.time()
        result = TaskResult(
            task_id=task.task_id,
            worker_id=self.worker_id,
            start_time=start_time
        )
        
        try:
            # Get the appropriate processor
            processor = self.task_processors.get(
                task.task_type,
                self._process_unknown_task
            )
            
            # Execute the task
            task_result = processor(task.payload)
            
            result.result = task_result
            result.status = TaskStatus.COMPLETED
            
        except Exception as e:
            result.error = str(e)
            result.status = TaskStatus.FAILED
        
        finally:
            result.end_time = time.time()
        
        return result
    
    def _process_compute_task(self, payload: Dict[str, Any]) -> Any:
        """Process compute-intensive tasks."""
        operation = payload.get("operation", "sum")
        data = payload.get("data", [])
        
        if operation == "sum":
            result = sum(data)
        elif operation == "mean":
            result = np.mean(data)
        elif operation == "matrix_multiply":
            matrix_a = np.array(payload.get("matrix_a", [[1, 2], [3, 4]]))
            matrix_b = np.array(payload.get("matrix_b", [[5, 6], [7, 8]]))
            result = np.matmul(matrix_a, matrix_b).tolist()
        else:
            result = f"Unknown operation: {operation}"
        
        # Simulate computation time
        time.sleep(random.uniform(0.1, 0.5))
        return result
    
    def _process_data_transform_task(self, payload: Dict[str, Any]) -> Any:
        """Process data transformation tasks."""
        transform_type = payload.get("transform", "uppercase")
        data = payload.get("data", "")
        
        if transform_type == "uppercase":
            result = data.upper()
        elif transform_type == "reverse":
            result = data[::-1]
        elif transform_type == "json_parse":
            result = json.loads(data)
        else:
            result = data
        
        # Simulate processing time
        time.sleep(random.uniform(0.05, 0.2))
        return result
    
    def _process_io_task(self, payload: Dict[str, Any]) -> Any:
        """Process I/O tasks."""
        # Simulate I/O operation
        time.sleep(random.uniform(0.2, 1.0))
        return f"Processed I/O task: {payload.get('description', 'unknown')}"
    
    def _process_unknown_task(self, payload: Dict[str, Any]) -> Any:
        """Handle unknown task types."""
        return f"Unknown task type processed with payload: {payload}"
    
    def get_stats(self) -> Dict:
        """Get worker statistics."""
        avg_time = (self.total_execution_time / self.tasks_processed 
                   if self.tasks_processed > 0 else 0)
        
        return {
            "worker_id": self.worker_id,
            "tasks_processed": self.tasks_processed,
            "total_execution_time": self.total_execution_time,
            "average_execution_time": avg_time
        }


def create_sample_tasks(num_tasks: int = 20) -> List[Task]:
    """Create sample tasks for demonstration."""
    tasks = []
    
    for i in range(num_tasks):
        # Mix of different task types
        task_type = random.choice(["compute", "data_transform", "io_task"])
        priority = random.choice(list(TaskPriority))
        
        if task_type == "compute":
            operation = random.choice(["sum", "mean", "matrix_multiply"])
            if operation == "matrix_multiply":
                payload = {
                    "operation": operation,
                    "matrix_a": [[random.randint(1, 10) for _ in range(3)] for _ in range(3)],
                    "matrix_b": [[random.randint(1, 10) for _ in range(3)] for _ in range(3)]
                }
            else:
                payload = {
                    "operation": operation,
                    "data": [random.randint(1, 100) for _ in range(10)]
                }
        elif task_type == "data_transform":
            transform = random.choice(["uppercase", "reverse", "json_parse"])
            if transform == "json_parse":
                payload = {
                    "transform": transform,
                    "data": json.dumps({"key": f"value_{i}", "number": i})
                }
            else:
                payload = {
                    "transform": transform,
                    "data": f"sample_text_{i}"
                }
        else:  # io_task
            payload = {
                "description": f"Read file_{i}.txt"
            }
        
        task = Task(
            task_id=f"task_{i:04d}",
            task_type=task_type,
            payload=payload,
            priority=priority
        )
        tasks.append(task)
    
    return tasks


def demo_task_queue():
    """Demonstrate the distributed task queue system."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Distributed Task Queue Demo ===\n")
    
    # Create task queue
    queue = TaskQueue.remote("main_queue")
    
    # Create sample tasks
    tasks = create_sample_tasks(30)
    
    # Submit tasks to queue
    print(f"Submitting {len(tasks)} tasks to queue...")
    for task in tasks:
        ray.get(queue.submit_task.remote(task))
    
    # Create workers
    num_workers = 4
    workers = []
    worker_refs = []
    
    print(f"Starting {num_workers} workers...")
    for i in range(num_workers):
        worker = TaskWorker.remote(f"worker_{i:02d}", queue)
        workers.append(worker)
        # Start worker in background
        worker_refs.append(worker.start.remote())
    
    # Monitor progress
    print("\nProcessing tasks...")
    for i in range(10):
        time.sleep(1)
        status = ray.get(queue.get_queue_status.remote())
        print(f"Queue status: Pending={status['pending']}, "
              f"Running={status['running']}, "
              f"Completed={status['completed']}, "
              f"Failed={status['failed']}")
        
        if status['pending'] == 0 and status['running'] == 0:
            break
    
    # Stop workers
    print("\nStopping workers...")
    for worker in workers:
        ray.get(worker.stop.remote())
    
    # Get final statistics
    print("\nFinal Statistics:")
    final_status = ray.get(queue.get_queue_status.remote())
    print(f"Total tasks processed: {final_status['total_processed']}")
    print(f"Completed: {final_status['completed']}")
    print(f"Failed: {final_status['failed']}")
    
    print("\nWorker Statistics:")
    for worker in workers:
        stats = ray.get(worker.get_stats.remote())
        print(f"  {stats['worker_id']}: "
              f"Processed {stats['tasks_processed']} tasks, "
              f"Avg time: {stats['average_execution_time']:.2f}s")
    
    ray.shutdown()


if __name__ == "__main__":
    demo_task_queue()
