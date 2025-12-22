"""
Ray Agent Demo - Optimized for High Performance
A collection of optimized distributed computing patterns using Ray.
"""

# Import optimized agents and components
from .basic_agent import BasicAgent
from .improved_agent import ImprovedAgent, AgentMonitor
from .multi_agent_coordinator import AgentCoordinator
from .distributed_task_queue import DistributedTaskQueue
from .smtp_agent import SMTPAgent, EmailMessage, create_smtp_agent, send_test_email

__version__ = "2.1.0"
__all__ = [
    "BasicAgent",
    "ImprovedAgent", 
    "AgentMonitor",
    "AgentCoordinator",
    "DistributedTaskQueue",
    "SMTPAgent",
    "EmailMessage", 
    "create_smtp_agent",
    "send_test_email"
]
