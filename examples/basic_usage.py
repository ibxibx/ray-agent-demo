"""Basic usage examples for ray-agent-demo."""

import ray
from ray_agent_demo import BasicAgent, AgentCoordinator

def simple_agent_example():
    """Simple example of using a basic agent."""
    ray.init(ignore_reinit_error=True)
    
    # Create and use an agent
    agent = BasicAgent.remote("my-agent")
    result = ray.get(agent.process_task.remote("Hello, Ray!"))
    print(f"Result: {result}")
    
    # Get agent status
    status = ray.get(agent.get_status.remote())
    print(f"Agent status: {status}")
    
    ray.shutdown()

def multi_agent_example():
    """Example of coordinating multiple agents."""
    ray.init(ignore_reinit_error=True)
    
    # Create coordinator with 3 agents
    coordinator = AgentCoordinator.remote(num_agents=3)
    
    # Process tasks
    tasks = [f"task_{i}" for i in range(10)]
    results = ray.get(coordinator.process_all_tasks.remote(tasks))
    
    print(f"Processed {len(results)} tasks")
    
    # Get summary
    summary = ray.get(coordinator.get_workload_summary.remote())
    print(f"Summary: {summary}")
    
    ray.shutdown()

if __name__ == "__main__":
    print("=== Simple Agent Example ===")
    simple_agent_example()
    
    print("\n=== Multi-Agent Example ===")
    multi_agent_example()
