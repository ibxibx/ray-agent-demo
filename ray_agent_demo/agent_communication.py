"""
Agent Communication Example
Demonstrates various patterns for agents to communicate with each other in Ray.
"""

import ray
import time
import asyncio
from typing import Any, Dict, List, Optional
from enum import Enum


class MessageType(Enum):
    """Types of messages agents can send."""
    TASK = "task"
    RESULT = "result"
    STATUS = "status"
    BROADCAST = "broadcast"
    PING = "ping"
    PONG = "pong"


@ray.remote
class Message:
    """Wrapper for messages passed between agents."""
    
    def __init__(self, sender_id: str, receiver_id: str, msg_type: MessageType, 
                 content: Any, timestamp: float = None):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.msg_type = msg_type
        self.content = content
        self.timestamp = timestamp or time.time()


@ray.remote
class CommunicatingAgent:
    """
    An agent that can send and receive messages from other agents.
    """
    
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.inbox = []
        self.outbox = []
        self.peers = {}  # agent_id -> agent_ref mapping
        self.processed_messages = 0
    
    def register_peer(self, peer_id: str, peer_ref: Any):
        """Register another agent as a peer for communication."""
        self.peers[peer_id] = peer_ref
        return f"{self.agent_id} registered peer: {peer_id}"
    
    def send_message(self, receiver_id: str, msg_type: MessageType, content: Any):
        """Send a message to another agent."""
        if receiver_id not in self.peers:
            return f"Unknown receiver: {receiver_id}"
        
        message = Message.remote(
            self.agent_id, receiver_id, msg_type, content
        )
        
        # Send to peer's inbox
        self.peers[receiver_id].receive_message.remote(message)
        self.outbox.append(message)
        
        return f"Message sent from {self.agent_id} to {receiver_id}"
    
    def receive_message(self, message: Any):
        """Receive a message from another agent."""
        self.inbox.append(message)
        return f"{self.agent_id} received message"
    
    def process_inbox(self) -> List[Dict]:
        """Process all messages in the inbox."""
        results = []
        
        for msg_ref in self.inbox:
            msg = ray.get(msg_ref)
            self.processed_messages += 1
            
            result = {
                "agent_id": self.agent_id,
                "processed_msg_from": msg.sender_id,
                "msg_type": msg.msg_type.value,
                "content": msg.content,
                "timestamp": msg.timestamp
            }
            
            # Handle different message types
            if msg.msg_type == MessageType.PING:
                # Respond to ping with pong
                self.send_message(msg.sender_id, MessageType.PONG, "pong")
                result["action"] = "sent_pong"
            elif msg.msg_type == MessageType.TASK:
                # Process task and send result back
                task_result = f"Processed: {msg.content}"
                self.send_message(msg.sender_id, MessageType.RESULT, task_result)
                result["action"] = "processed_task"
            
            results.append(result)
        
        # Clear processed messages
        self.inbox.clear()
        return results
    
    def broadcast_message(self, msg_type: MessageType, content: Any):
        """Broadcast a message to all peers."""
        for peer_id in self.peers:
            self.send_message(peer_id, msg_type, content)
        return f"{self.agent_id} broadcasted to {len(self.peers)} peers"
    
    def get_status(self) -> Dict:
        """Get current agent status."""
        return {
            "agent_id": self.agent_id,
            "inbox_size": len(self.inbox),
            "outbox_size": len(self.outbox),
            "num_peers": len(self.peers),
            "processed_messages": self.processed_messages
        }


@ray.remote
class MessageRouter:
    """
    Central message router for managing agent communication.
    Implements a pub/sub pattern.
    """
    
    def __init__(self):
        self.agents = {}
        self.topics = {}  # topic -> list of subscriber agent_ids
        self.message_log = []
    
    def register_agent(self, agent_id: str, agent_ref: Any):
        """Register an agent with the router."""
        self.agents[agent_id] = agent_ref
        return f"Registered agent: {agent_id}"
    
    def subscribe_to_topic(self, agent_id: str, topic: str):
        """Subscribe an agent to a topic."""
        if topic not in self.topics:
            self.topics[topic] = []
        
        if agent_id not in self.topics[topic]:
            self.topics[topic].append(agent_id)
        
        return f"{agent_id} subscribed to topic: {topic}"
    
    def publish_to_topic(self, topic: str, sender_id: str, content: Any):
        """Publish a message to all subscribers of a topic."""
        if topic not in self.topics:
            return f"No subscribers for topic: {topic}"
        
        subscribers = self.topics[topic]
        message = Message.remote(
            sender_id, f"topic:{topic}", MessageType.BROADCAST, content
        )
        
        # Send to all subscribers
        for subscriber_id in subscribers:
            if subscriber_id != sender_id and subscriber_id in self.agents:
                self.agents[subscriber_id].receive_message.remote(message)
        
        self.message_log.append({
            "topic": topic,
            "sender": sender_id,
            "num_recipients": len(subscribers) - 1,
            "timestamp": time.time()
        })
        
        return f"Published to {len(subscribers) - 1} subscribers"
    
    def get_stats(self) -> Dict:
        """Get router statistics."""
        return {
            "num_agents": len(self.agents),
            "num_topics": len(self.topics),
            "total_messages_routed": len(self.message_log),
            "topics": {topic: len(subs) for topic, subs in self.topics.items()}
        }


def demo_direct_communication():
    """Demonstrate direct agent-to-agent communication."""
    ray.init(ignore_reinit_error=True)
    
    # Create three communicating agents
    agent1 = CommunicatingAgent.remote("agent-001")
    agent2 = CommunicatingAgent.remote("agent-002")
    agent3 = CommunicatingAgent.remote("agent-003")
    
    # Register peers
    ray.get([
        agent1.register_peer.remote("agent-002", agent2),
        agent1.register_peer.remote("agent-003", agent3),
        agent2.register_peer.remote("agent-001", agent1),
        agent2.register_peer.remote("agent-003", agent3),
        agent3.register_peer.remote("agent-001", agent1),
        agent3.register_peer.remote("agent-002", agent2),
    ])
    
    print("=== Direct Communication Demo ===")
    
    # Agent 1 sends a task to Agent 2
    print("\n1. Agent 1 sends task to Agent 2:")
    result = ray.get(agent1.send_message.remote("agent-002", MessageType.TASK, "compute_sum(1, 2)"))
    print(f"   {result}")
    
    # Agent 2 processes its inbox
    time.sleep(0.1)
    processed = ray.get(agent2.process_inbox.remote())
    print(f"   Agent 2 processed: {processed}")
    
    # Agent 1 broadcasts to all peers
    print("\n2. Agent 1 broadcasts status:")
    result = ray.get(agent1.broadcast_message.remote(MessageType.STATUS, "I'm busy"))
    print(f"   {result}")
    
    # All agents process their inboxes
    time.sleep(0.1)
    for agent in [agent2, agent3]:
        processed = ray.get(agent.process_inbox.remote())
        if processed:
            print(f"   {processed[0]['agent_id']} received broadcast")
    
    # Ping-pong test
    print("\n3. Ping-pong test:")
    ray.get(agent1.send_message.remote("agent-002", MessageType.PING, "ping"))
    time.sleep(0.1)
    ray.get(agent2.process_inbox.remote())
    time.sleep(0.1)
    pong_msgs = ray.get(agent1.process_inbox.remote())
    if pong_msgs:
        print(f"   Agent 1 received: {pong_msgs[0]['content']}")
    
    ray.shutdown()


def demo_pubsub_pattern():
    """Demonstrate publish-subscribe communication pattern."""
    ray.init(ignore_reinit_error=True)
    
    # Create message router
    router = MessageRouter.remote()
    
    # Create agents
    agents = []
    agent_refs = []
    for i in range(4):
        agent_id = f"agent-{i:03d}"
        agent = CommunicatingAgent.remote(agent_id)
        agents.append((agent_id, agent))
        agent_refs.append(agent)
    
    # Register agents with router
    for agent_id, agent in agents:
        ray.get(router.register_agent.remote(agent_id, agent))
    
    print("=== Pub/Sub Pattern Demo ===")
    
    # Set up subscriptions
    ray.get([
        router.subscribe_to_topic.remote("agent-000", "tasks"),
        router.subscribe_to_topic.remote("agent-001", "tasks"),
        router.subscribe_to_topic.remote("agent-002", "tasks"),
        router.subscribe_to_topic.remote("agent-001", "status"),
        router.subscribe_to_topic.remote("agent-003", "status"),
    ])
    
    # Publish to 'tasks' topic
    print("\n1. Publishing to 'tasks' topic:")
    result = ray.get(router.publish_to_topic.remote("tasks", "agent-003", "new_batch_available"))
    print(f"   {result}")
    
    # Let subscribers process messages
    time.sleep(0.1)
    for i in range(3):  # First 3 agents subscribed to 'tasks'
        agent = agent_refs[i]
        inbox_size = ray.get(agent.get_status.remote())["inbox_size"]
        if inbox_size > 0:
            print(f"   agent-{i:03d} has {inbox_size} new message(s)")
    
    # Get router stats
    stats = ray.get(router.get_stats.remote())
    print(f"\n2. Router statistics:")
    print(f"   Active agents: {stats['num_agents']}")
    print(f"   Active topics: {stats['topics']}")
    print(f"   Total messages routed: {stats['total_messages_routed']}")
    
    ray.shutdown()


if __name__ == "__main__":
    demo_direct_communication()
    print("\n" + "="*50 + "\n")
    demo_pubsub_pattern()
