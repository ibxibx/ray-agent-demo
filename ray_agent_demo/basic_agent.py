"""
Optimized Basic Ray Agent
High-performance Ray actor for efficient task processing.
"""

import ray
import time
from typing import Any, List, Dict
from collections import deque


@ray.remote
class BasicAgent:
    """
    Optimized Ray actor with minimal overhead.
    """
    
    def __init__(self, agent_id: str, max_results: int = 10000):
        self.agent_id = agent_id
        self.tasks_processed = 0
        # Use deque for O(1) append with automatic size limit
        self.results = deque(maxlen=max_results)
        
    def process_task(self, task: Any) -> Dict:
        """
        Process a task with minimal overhead.
        """
        # Remove unnecessary sleep for production use
        # Only keep if you need to simulate processing time
        # time.sleep(0.01)
        
        self.tasks_processed += 1
        
        result = {
            "agent_id": self.agent_id,
            "task": task,
            "task_number": self.tasks_processed,
            "timestamp": time.perf_counter()  # More precise timing
        }
        
        self.results.append(result)
        return result
    
    def process_batch(self, tasks: List[Any]) -> List[Dict]:
        """
        Process multiple tasks efficiently.
        """
        return [self.process_task(task) for task in tasks]
    
    def get_status(self) -> Dict:
        """
        Get agent status efficiently.
        """
        return {
            "agent_id": self.agent_id,
            "tasks_processed": self.tasks_processed,
            "is_alive": True,
            "results_stored": len(self.results)
        }
    
    def get_all_results(self) -> List[Dict]:
        """
        Get all stored results.
        """
        return list(self.results)
    
    def clear_results(self):
        """
        Clear stored results to free memory.
        """
        self.results.clear()
        return f"Cleared results for {self.agent_id}"
