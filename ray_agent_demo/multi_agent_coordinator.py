"""
Multi-Agent Coordinator Example
Demonstrates how to coordinate multiple Ray agents to process tasks in parallel.
"""

import ray
import time
import random
from typing import List, Dict, Any
from basic_agent import BasicAgent


@ray.remote
class AgentCoordinator:
    """
    Coordinates multiple agents to distribute and process tasks efficiently.
    """
    
    def __init__(self, num_agents: int = 3):
        self.num_agents = num_agents
        self.agents = []
        self.task_assignments = {}
        self.completed_tasks = []
        
        # Create the agent pool
        for i in range(num_agents):
            agent = BasicAgent.remote(f"agent-{i:03d}")
            self.agents.append(agent)
    
    def distribute_tasks(self, tasks: List[Any]) -> Dict[str, List[Any]]:
        """
        Distribute tasks among available agents using round-robin.
        
        Args:
            tasks: List of tasks to distribute
            
        Returns:
            Dict mapping agent IDs to their assigned tasks
        """
        assignments = {f"agent-{i:03d}": [] for i in range(self.num_agents)}
        
        for idx, task in enumerate(tasks):
            agent_idx = idx % self.num_agents
            agent_id = f"agent-{agent_idx:03d}"
            assignments[agent_id].append(task)
            
        self.task_assignments = assignments
        return assignments
    
    def process_all_tasks(self, tasks: List[Any]) -> List[Dict]:
        """
        Process all tasks using the agent pool.
        
        Args:
            tasks: List of tasks to process
            
        Returns:
            List of all results
        """
        # Distribute tasks
        assignments = self.distribute_tasks(tasks)
        
        # Submit tasks to agents
        futures = []
        for agent_idx, agent in enumerate(self.agents):
            agent_id = f"agent-{agent_idx:03d}"
            agent_tasks = assignments[agent_id]
            
            for task in agent_tasks:
                future = agent.process_task.remote(task)
                futures.append(future)
        
        # Collect all results
        results = ray.get(futures)
        self.completed_tasks.extend(results)
        
        return results
    
    def get_agent_statuses(self) -> List[Dict]:
        """Get status of all agents."""
        status_futures = [agent.get_status.remote() for agent in self.agents]
        return ray.get(status_futures)
    
    def get_workload_summary(self) -> Dict:
        """Get summary of workload distribution."""
        statuses = self.get_agent_statuses()
        
        total_tasks = sum(status["tasks_processed"] for status in statuses)
        avg_tasks = total_tasks / self.num_agents if self.num_agents > 0 else 0
        
        return {
            "num_agents": self.num_agents,
            "total_tasks_completed": total_tasks,
            "average_tasks_per_agent": avg_tasks,
            "task_assignments": self.task_assignments,
            "agent_statuses": statuses
        }


@ray.remote
class LoadBalancedCoordinator(AgentCoordinator):
    """
    Enhanced coordinator with load balancing capabilities.
    """
    
    def __init__(self, num_agents: int = 3):
        super().__init__(num_agents)
        self.agent_loads = {i: 0 for i in range(num_agents)}
    
    def get_least_loaded_agent(self) -> int:
        """Find the agent with the least number of assigned tasks."""
        return min(self.agent_loads.items(), key=lambda x: x[1])[0]
    
    def distribute_tasks_balanced(self, tasks: List[Any]) -> Dict[str, List[Any]]:
        """
        Distribute tasks using load balancing.
        
        Args:
            tasks: List of tasks to distribute
            
        Returns:
            Dict mapping agent IDs to their assigned tasks
        """
        assignments = {f"agent-{i:03d}": [] for i in range(self.num_agents)}
        
        # Simulate task weights (in real scenarios, you might estimate task complexity)
        task_weights = {task: random.uniform(0.5, 2.0) for task in tasks}
        
        # Sort tasks by weight (heaviest first for better distribution)
        sorted_tasks = sorted(tasks, key=lambda t: task_weights[t], reverse=True)
        
        for task in sorted_tasks:
            agent_idx = self.get_least_loaded_agent()
            agent_id = f"agent-{agent_idx:03d}"
            assignments[agent_id].append(task)
            self.agent_loads[agent_idx] += task_weights[task]
        
        self.task_assignments = assignments
        return assignments


def demo_multi_agent():
    """Demonstrate multi-agent coordination."""
    ray.init(ignore_reinit_error=True)
    
    # Create coordinator with 4 agents
    coordinator = AgentCoordinator.remote(num_agents=4)
    
    # Create a batch of tasks
    tasks = [f"task_{i}" for i in range(20)]
    
    print("Starting multi-agent processing...")
    start_time = time.time()
    
    # Process all tasks
    results = ray.get(coordinator.process_all_tasks.remote(tasks))
    
    # Get workload summary
    summary = ray.get(coordinator.get_workload_summary.remote())
    
    end_time = time.time()
    
    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds")
    print(f"\nWorkload Summary:")
    print(f"  Total agents: {summary['num_agents']}")
    print(f"  Total tasks completed: {summary['total_tasks_completed']}")
    print(f"  Average tasks per agent: {summary['average_tasks_per_agent']:.2f}")
    
    print("\nAgent Statuses:")
    for status in summary['agent_statuses']:
        print(f"  {status['agent_id']}: {status['tasks_processed']} tasks processed")
    
    ray.shutdown()


def demo_load_balanced():
    """Demonstrate load-balanced multi-agent coordination."""
    ray.init(ignore_reinit_error=True)
    
    # Create load-balanced coordinator
    coordinator = LoadBalancedCoordinator.remote(num_agents=3)
    
    # Create tasks with varying complexity
    tasks = [f"heavy_task_{i}" for i in range(5)] + [f"light_task_{i}" for i in range(15)]
    
    print("Starting load-balanced processing...")
    
    # Distribute tasks with load balancing
    assignments = ray.get(coordinator.distribute_tasks_balanced.remote(tasks))
    
    print("\nLoad-balanced task assignments:")
    for agent_id, agent_tasks in assignments.items():
        print(f"  {agent_id}: {len(agent_tasks)} tasks - {agent_tasks}")
    
    ray.shutdown()


if __name__ == "__main__":
    print("=== Basic Multi-Agent Demo ===")
    demo_multi_agent()
    
    print("\n\n=== Load-Balanced Demo ===")
    demo_load_balanced()
