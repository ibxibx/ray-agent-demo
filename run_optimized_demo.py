"""
Optimized Ray Agent Demo Runner
Demonstrates high-performance distributed computing with Ray agents.
"""

import ray
import time
import sys
from ray_agent_demo import BasicAgent, ImprovedAgent, AgentMonitor, AgentCoordinator


def run_performance_benchmark():
    """Run performance benchmark with optimized agents."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Ray Agent Performance Benchmark ===\n")
    
    # Test 1: Single agent throughput
    print("1. Single Agent Throughput Test")
    agent = ImprovedAgent.remote("perf-agent", max_concurrent_tasks=10)
    
    num_tasks = 1000
    tasks = [{"operation": "sum", "data": list(range(10))} for _ in range(num_tasks)]
    
    start_time = time.perf_counter()
    results = ray.get(agent.process_batch.remote(tasks))
    end_time = time.perf_counter()
    
    duration = end_time - start_time
    throughput = num_tasks / duration
    
    print(f"   Processed {num_tasks} tasks in {duration:.3f}s")
    print(f"   Throughput: {throughput:.1f} tasks/second\n")
    
    # Test 2: Multi-agent scaling
    print("2. Multi-Agent Scaling Test")
    for num_agents in [1, 2, 4, 8]:
        coordinator = AgentCoordinator.remote(num_agents=num_agents)
        
        start_time = time.perf_counter()
        results = ray.get(coordinator.process_all_tasks.remote(tasks))
        end_time = time.perf_counter()
        
        duration = end_time - start_time
        throughput = num_tasks / duration
        
        print(f"   {num_agents} agents: {duration:.3f}s ({throughput:.1f} tasks/sec)")
    
    # Test 3: Concurrent operations
    print("\n3. Concurrent Operations Test")
    agents = [ImprovedAgent.remote(f"concurrent-{i}", max_concurrent_tasks=20) 
              for i in range(4)]
    
    # Different operation types
    operation_tasks = [
        [{"operation": "sum", "data": list(range(20))} for _ in range(250)],
        [{"operation": "multiply", "data": [2, 3, 4]} for _ in range(250)],
        [{"operation": "filter", "data": list(range(-10, 11))} for _ in range(250)],
        [{"operation": "transform", "data": list(range(10))} for _ in range(250)]
    ]
    
    start_time = time.perf_counter()
    futures = []
    for i, agent in enumerate(agents):
        futures.append(agent.process_batch.remote(operation_tasks[i]))
    
    all_results = ray.get(futures)
    end_time = time.perf_counter()
    
    total_processed = sum(len(results) for results in all_results)
    duration = end_time - start_time
    
    print(f"   Processed {total_processed} mixed operations in {duration:.3f}s")
    print(f"   Aggregate throughput: {total_processed/duration:.1f} ops/second")
    
    ray.shutdown()
    print("\n✅ Benchmark complete!")


def run_quick_demo():
    """Run a quick demo of the optimized agents."""
    ray.init(ignore_reinit_error=True)
    
    print("=== Quick Agent Demo ===\n")
    
    # Create an optimized agent
    agent = ImprovedAgent.remote("demo-agent")
    
    # Process various tasks
    tasks = [
        {"operation": "sum", "data": [1, 2, 3, 4, 5]},
        {"operation": "multiply", "data": [2, 3, 4]},
        {"operation": "filter", "data": list(range(-5, 6))},
        {"operation": "transform", "data": [1, 2, 3]}
    ]
    
    print("Processing tasks...")
    results = ray.get(agent.process_batch.remote(tasks))
    
    for i, result in enumerate(results):
        if result["success"]:
            print(f"Task {i+1}: {tasks[i]['operation']} → {result['result']}")
    
    # Get agent status
    status = ray.get(agent.get_status.remote())
    print(f"\nAgent Status: {status['tasks_processed']} tasks processed")
    print(f"Success Rate: {status['success_rate']*100:.1f}%")
    print(f"Avg Duration: {status['average_duration']*1000:.1f}ms")
    
    ray.shutdown()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "benchmark":
        run_performance_benchmark()
    else:
        run_quick_demo()
        print("\nRun with 'benchmark' argument for performance testing:")
        print("  python run_optimized_demo.py benchmark")
