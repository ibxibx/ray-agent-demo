"""
Ray Agent Demo Package
A comprehensive demonstration of Ray agent patterns and distributed computing.
"""

__version__ = "0.2.0"

# Import main classes and functions for easier access
from .basic_agent import BasicAgent, demo_basic_agent
from .multi_agent_coordinator import (
    AgentCoordinator, 
    LoadBalancedCoordinator,
    demo_multi_agent,
    demo_load_balanced
)
from .agent_communication import (
    CommunicatingAgent,
    MessageRouter,
    MessageType,
    Message,
    demo_direct_communication,
    demo_pubsub_pattern
)
from .distributed_task_queue import (
    TaskQueue,
    TaskWorker,
    Task,
    TaskResult,
    TaskStatus,
    TaskPriority,
    create_sample_tasks,
    demo_task_queue
)
from .improved_agent import (
    ImprovedAgent,
    AgentMonitor,
    TaskMetrics,
    demo_improved_agent,
    demo_monitoring
)

__all__ = [
    # Basic Agent
    "BasicAgent",
    "demo_basic_agent",
    
    # Multi-Agent Coordinator
    "AgentCoordinator",
    "LoadBalancedCoordinator",
    "demo_multi_agent",
    "demo_load_balanced",
    
    # Agent Communication
    "CommunicatingAgent",
    "MessageRouter",
    "MessageType",
    "Message",
    "demo_direct_communication",
    "demo_pubsub_pattern",
    
    # Distributed Task Queue
    "TaskQueue",
    "TaskWorker",
    "Task",
    "TaskResult",
    "TaskStatus",
    "TaskPriority",
    "create_sample_tasks",
    "demo_task_queue",
    
    # Improved Agent
    "ImprovedAgent",
    "AgentMonitor",
    "TaskMetrics",
    "demo_improved_agent",
    "demo_monitoring",
]
