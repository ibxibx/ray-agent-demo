"""
Basic Ray Agent Example
This module demonstrates a simple Ray actor (agent) that can process tasks.
"""

import ray
import time
from typing import Any, List


@ray.remote
class BasicAgent:
    """
    A basic Ray actor that simulates an agent processing tasks.
    
    This agent demonstrates:
    - State management in Ray actors
    - Asynchronous task processing
    - Result accumulation
    """
    
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.tasks_processed = 0
        self.results = []
        
    def process_task(self, task: Any) -> dict:
        """
        Process a single task and return the result.
        
        Args:
            task: The task to process (can be any type)
            
        Returns:
            dict: Result containing task info and processing details
        """
        # Simulate task processing time
        time.sleep(0.1)
        
        self.tasks_processed += 1
        result = {
            "agent_id": self.agent_id,
            "task": task,
            "task_number": self.tasks_processed,
            "timestamp": time.time()
        }
        
        self.results.append(result)
        return result
    
    def get_status(self) -> dict:
        """
        Get the current status of the agent.
        
        Returns:
            dict: Current agent status
        """
        return {
            "agent_id": self.agent_id,
            "tasks_processed": self.tasks_processed,
            "is_alive": True
        }
    
    def get_all_results(self) -> List[dict]:
        """
        Retrieve all results processed by this agent.
        
        Returns:
            List[dict]: All processing results
        """
        return self.results


def demo_basic_agent():
    """Demonstrate basic agent functionality."""
    # Initialize Ray
    ray.init(ignore_reinit_error=True)
    
    # Create an agent
    agent = BasicAgent.remote("agent-001")
    
    # Process some tasks asynchronously
    tasks = ["task_1", "task_2", "task_3", "task_4", "task_5"]
    futures = []
    
    print("Submitting tasks to agent...")
    for task in tasks:
        future = agent.process_task.remote(task)
        futures.append(future)
    
    # Wait for all tasks to complete and get results
    print("Waiting for results...")
    results = ray.get(futures)
    
    # Get agent status and all results
    status = ray.get(agent.get_status.remote())
    all_results = ray.get(agent.get_all_results.remote())
    
    print("\nAgent Status:")
    print(status)
    
    print("\nTask Results:")
    for res in all_results:
        print(res)
    
    # Shutdown Ray
    ray.shutdown()


if __name__ == "__main__":
    demo_basic_agent()
