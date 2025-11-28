"""
Test suite for Ray Agent Demo
Run with: pytest test_agents.py -v
"""

import pytest
import ray
import time
import asyncio
from typing import List

# Import the modules to test
from ray_agent_demo.basic_agent import BasicAgent, demo_basic_agent
from ray_agent_demo.multi_agent_coordinator import AgentCoordinator, LoadBalancedCoordinator
from ray_agent_demo.agent_communication import CommunicatingAgent, MessageRouter, MessageType
from ray_agent_demo.distributed_task_queue import TaskQueue, TaskWorker, Task, TaskStatus, TaskPriority
from ray_agent_demo.improved_agent import ImprovedAgent, AgentMonitor


@pytest.fixture(scope="function")
def ray_init():
    """Initialize Ray for each test."""
    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


class TestBasicAgent:
    """Test basic agent functionality."""
    
    def test_agent_creation(self, ray_init):
        """Test creating a basic agent."""
        agent = BasicAgent.remote("test-agent")
        status = ray.get(agent.get_status.remote())
        
        assert status["agent_id"] == "test-agent"
        assert status["tasks_processed"] == 0
        assert status["is_alive"] == True
    
    def test_task_processing(self, ray_init):
        """Test processing a single task."""
        agent = BasicAgent.remote("test-agent")
        result = ray.get(agent.process_task.remote("test_task"))
        
        assert result["agent_id"] == "test-agent"
        assert result["task"] == "test_task"
        assert result["task_number"] == 1
        assert "timestamp" in result
    
    def test_multiple_tasks(self, ray_init):
        """Test processing multiple tasks."""
        agent = BasicAgent.remote("test-agent")
        tasks = ["task_1", "task_2", "task_3"]
        
        results = []
        for task in tasks:
            result = ray.get(agent.process_task.remote(task))
            results.append(result)
        
        assert len(results) == 3
        assert all(r["agent_id"] == "test-agent" for r in results)
        assert [r["task_number"] for r in results] == [1, 2, 3]
    
    def test_get_all_results(self, ray_init):
        """Test retrieving all results."""
        agent = BasicAgent.remote("test-agent")
        tasks = ["task_1", "task_2"]
        
        for task in tasks:
            ray.get(agent.process_task.remote(task))
        
        all_results = ray.get(agent.get_all_results.remote())
        assert len(all_results) == 2
        assert all_results[0]["task"] == "task_1"
        assert all_results[1]["task"] == "task_2"


class TestMultiAgentCoordinator:
    """Test multi-agent coordination."""
    
    def test_coordinator_creation(self, ray_init):
        """Test creating a coordinator with multiple agents."""
        coordinator = AgentCoordinator.remote(num_agents=3)
        summary = ray.get(coordinator.get_workload_summary.remote())
        
        assert summary["num_agents"] == 3
        assert summary["total_tasks_completed"] == 0
    
    def test_task_distribution(self, ray_init):
        """Test distributing tasks among agents."""
        coordinator = AgentCoordinator.remote(num_agents=3)
        tasks = [f"task_{i}" for i in range(9)]
        
        assignments = ray.get(coordinator.distribute_tasks.remote(tasks))
        
        # Check round-robin distribution
        assert len(assignments) == 3
        assert all(len(tasks) == 3 for tasks in assignments.values())
        assert assignments["agent-000"] == ["task_0", "task_3", "task_6"]
    
    def test_process_all_tasks(self, ray_init):
        """Test processing all tasks through coordinator."""
        coordinator = AgentCoordinator.remote(num_agents=2)
        tasks = [f"task_{i}" for i in range(4)]
        
        results = ray.get(coordinator.process_all_tasks.remote(tasks))
        
        assert len(results) == 4
        assert all("agent_id" in r for r in results)
        assert all("timestamp" in r for r in results)
    
    def test_load_balanced_coordinator(self, ray_init):
        """Test load-balanced task distribution."""
        coordinator = LoadBalancedCoordinator.remote(num_agents=2)
        tasks = [f"task_{i}" for i in range(6)]
        
        assignments = ray.get(coordinator.distribute_tasks_balanced.remote(tasks))
        
        # Should distribute evenly for similar weighted tasks
        assert len(assignments["agent-000"]) in [2, 3, 4]
        assert len(assignments["agent-001"]) in [2, 3, 4]
        assert sum(len(t) for t in assignments.values()) == 6


class TestAgentCommunication:
    """Test agent communication patterns."""
    
    def test_agent_peer_registration(self, ray_init):
        """Test registering peers for communication."""
        agent1 = CommunicatingAgent.remote("agent-1")
        agent2 = CommunicatingAgent.remote("agent-2")
        
        result = ray.get(agent1.register_peer.remote("agent-2", agent2))
        assert "registered peer" in result
        
        status = ray.get(agent1.get_status.remote())
        assert status["num_peers"] == 1
    
    def test_direct_messaging(self, ray_init):
        """Test sending messages between agents."""
        agent1 = CommunicatingAgent.remote("agent-1")
        agent2 = CommunicatingAgent.remote("agent-2")
        
        # Register peers
        ray.get(agent1.register_peer.remote("agent-2", agent2))
        ray.get(agent2.register_peer.remote("agent-1", agent1))
        
        # Send message
        result = ray.get(agent1.send_message.remote("agent-2", MessageType.TASK, "test_task"))
        assert "Message sent" in result
        
        # Check inbox
        time.sleep(0.1)
        status = ray.get(agent2.get_status.remote())
        assert status["inbox_size"] == 1
    
    def test_broadcast_messaging(self, ray_init):
        """Test broadcasting to multiple peers."""
        agents = [CommunicatingAgent.remote(f"agent-{i}") for i in range(3)]
        
        # Register all peers with agent 0
        for i in range(1, 3):
            ray.get(agents[0].register_peer.remote(f"agent-{i}", agents[i]))
        
        # Broadcast from agent 0
        ray.get(agents[0].broadcast_message.remote(MessageType.STATUS, "hello all"))
        
        # Check all other agents received message
        time.sleep(0.1)
        for i in range(1, 3):
            status = ray.get(agents[i].get_status.remote())
            assert status["inbox_size"] == 1
    
    def test_message_router(self, ray_init):
        """Test pub/sub pattern with message router."""
        router = MessageRouter.remote()
        agents = [CommunicatingAgent.remote(f"agent-{i}") for i in range(3)]
        
        # Register agents
        for i, agent in enumerate(agents):
            ray.get(router.register_agent.remote(f"agent-{i}", agent))
        
        # Subscribe to topic
        ray.get(router.subscribe_to_topic.remote("agent-0", "test_topic"))
        ray.get(router.subscribe_to_topic.remote("agent-1", "test_topic"))
        
        # Publish to topic
        result = ray.get(router.publish_to_topic.remote("test_topic", "agent-2", "test message"))
        assert "Published to 2 subscribers" in result
        
        # Check subscribers received message
        time.sleep(0.1)
        for i in range(2):
            status = ray.get(agents[i].get_status.remote())
            assert status["inbox_size"] == 1


class TestDistributedTaskQueue:
    """Test distributed task queue functionality."""
    
    def test_task_queue_creation(self, ray_init):
        """Test creating a task queue."""
        queue = TaskQueue.remote("test_queue")
        status = ray.get(queue.get_queue_status.remote())
        
        assert status["queue_name"] == "test_queue"
        assert status["pending"] == 0
        assert status["running"] == 0
        assert status["completed"] == 0
    
    def test_task_submission(self, ray_init):
        """Test submitting tasks to queue."""
        queue = TaskQueue.remote("test_queue")
        
        task = Task(
            task_id="test_task_001",
            task_type="compute",
            payload={"operation": "sum", "data": [1, 2, 3]}
        )
        
        task_id = ray.get(queue.submit_task.remote(task))
        assert task_id == "test_task_001"
        
        status = ray.get(queue.get_queue_status.remote())
        assert status["pending"] == 1
    
    def test_priority_ordering(self, ray_init):
        """Test task priority ordering."""
        queue = TaskQueue.remote("test_queue")
        
        # Submit tasks with different priorities
        tasks = [
            Task("task_1", "compute", {}, priority=TaskPriority.LOW),
            Task("task_2", "compute", {}, priority=TaskPriority.HIGH),
            Task("task_3", "compute", {}, priority=TaskPriority.NORMAL),
        ]
        
        for task in tasks:
            ray.get(queue.submit_task.remote(task))
        
        # Get tasks in order
        task1 = ray.get(queue.get_next_task.remote("worker_1"))
        task2 = ray.get(queue.get_next_task.remote("worker_2"))
        task3 = ray.get(queue.get_next_task.remote("worker_3"))
        
        assert task1.task_id == "task_2"  # HIGH priority
        assert task2.task_id == "task_3"  # NORMAL priority
        assert task3.task_id == "task_1"  # LOW priority
    
    def test_task_worker(self, ray_init):
        """Test task worker processing."""
        queue = TaskQueue.remote("test_queue")
        worker = TaskWorker.remote("test_worker", queue)
        
        # Submit a task
        task = Task(
            task_id="compute_task",
            task_type="compute",
            payload={"operation": "sum", "data": [1, 2, 3, 4, 5]}
        )
        ray.get(queue.submit_task.remote(task))
        
        # Process task
        # Note: In real scenario, worker.start() runs in a loop
        # For testing, we'll manually get and process the task
        task_to_process = ray.get(queue.get_next_task.remote("test_worker"))
        assert task_to_process is not None
        
        # Check worker stats
        stats = ray.get(worker.get_stats.remote())
        assert stats["worker_id"] == "test_worker"
        assert stats["tasks_processed"] == 0  # Not processed yet in our test


class TestImprovedAgent:
    """Test improved agent features."""
    
    def test_improved_agent_creation(self, ray_init):
        """Test creating an improved agent."""
        agent = ImprovedAgent.remote("improved-test", max_concurrent_tasks=5)
        status = ray.get(agent.get_status.remote())
        
        assert status["agent_id"] == "improved-test"
        assert status["max_concurrent_tasks"] == 5
        assert status["tasks_processed"] == 0
        assert status["tasks_failed"] == 0
    
    def test_async_task_processing(self, ray_init):
        """Test async task processing."""
        agent = ImprovedAgent.remote("async-test")
        
        task = {"operation": "sum", "data": [1, 2, 3, 4, 5]}
        result = ray.get(agent.process_task.remote(task))
        
        assert result["success"] == True
        assert result["result"] == 15
        assert "timestamp" in result
    
    def test_error_handling(self, ray_init):
        """Test error handling in task processing."""
        agent = ImprovedAgent.remote("error-test")
        
        # Submit task that will fail
        task = {"operation": "unknown_op", "data": [1, 2, 3]}
        result = ray.get(agent.process_task.remote(task))
        
        assert result["success"] == False
        assert "error" in result
        assert "Unknown operation" in result["error"]
        
        # Check failed task count
        status = ray.get(agent.get_status.remote())
        assert status["tasks_failed"] == 1
    
    def test_batch_processing(self, ray_init):
        """Test batch task processing."""
        agent = ImprovedAgent.remote("batch-test")
        
        tasks = [
            {"operation": "sum", "data": [1, 2, 3]},
            {"operation": "multiply", "data": [2, 3, 4]},
            "simple_string_task"
        ]
        
        results = ray.get(agent.process_batch.remote(tasks))
        
        assert len(results) == 3
        assert results[0]["result"] == 6  # sum
        assert results[1]["result"] == 24  # multiply
        assert "Processed: simple_string_task" in results[2]["result"]
    
    def test_metrics_collection(self, ray_init):
        """Test metrics collection."""
        agent = ImprovedAgent.remote("metrics-test")
        
        # Process some tasks
        for i in range(3):
            ray.get(agent.process_task.remote(f"task_{i}"))
        
        metrics = ray.get(agent.get_metrics.remote())
        assert len(metrics) == 3
        
        # Check metrics structure
        for metric in metrics:
            assert "task_id" in metric
            assert "start_time" in metric
            assert "end_time" in metric
            assert "success" in metric
            assert "duration" in metric
    
    def test_agent_monitoring(self, ray_init):
        """Test agent monitoring capabilities."""
        monitor = AgentMonitor.remote()
        agent = ImprovedAgent.remote("monitored-agent")
        
        # Register agent
        result = ray.get(monitor.register_agent.remote("monitored-agent", agent))
        assert "Registered agent" in result
        
        # Get monitoring summary
        summary = ray.get(monitor.get_summary.remote())
        assert summary["num_agents"] == 1
        
        # Process some tasks on the agent
        ray.get(agent.process_task.remote("test_task"))
        
        # Check updated summary
        # Note: In real scenario, monitor would run async monitoring
        # For testing, we'll just verify the structure
        assert "num_agents" in summary


# Performance benchmarks
class TestPerformance:
    """Performance benchmarks for agent systems."""
    
    def test_basic_agent_throughput(self, ray_init):
        """Measure basic agent throughput."""
        agent = BasicAgent.remote("perf-test")
        num_tasks = 100
        
        start_time = time.time()
        
        # Submit all tasks
        futures = [agent.process_task.remote(f"task_{i}") for i in range(num_tasks)]
        
        # Wait for completion
        results = ray.get(futures)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_tasks / duration
        
        print(f"\nBasic Agent Performance:")
        print(f"  Tasks: {num_tasks}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Throughput: {throughput:.2f} tasks/second")
        
        assert len(results) == num_tasks
        assert throughput > 10  # Should handle at least 10 tasks/second
    
    def test_multi_agent_scaling(self, ray_init):
        """Test scaling with multiple agents."""
        num_agents_list = [1, 2, 4]
        num_tasks = 100
        
        print("\nMulti-Agent Scaling:")
        
        for num_agents in num_agents_list:
            coordinator = AgentCoordinator.remote(num_agents=num_agents)
            tasks = [f"task_{i}" for i in range(num_tasks)]
            
            start_time = time.time()
            results = ray.get(coordinator.process_all_tasks.remote(tasks))
            end_time = time.time()
            
            duration = end_time - start_time
            throughput = num_tasks / duration
            
            print(f"  Agents: {num_agents}, Duration: {duration:.2f}s, "
                  f"Throughput: {throughput:.2f} tasks/second")
            
            assert len(results) == num_tasks


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
