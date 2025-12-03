# Ray Agent Demo - Optimized with SMTP Integration

A comprehensive demonstration of distributed computing with Ray, featuring optimized agent patterns, multi-agent coordination, and integrated email notifications via SMTP.

## 🚀 New Features

- **SMTP Email Agent**: Send emails asynchronously using Ray actors
- **Optimized Agent System**: High-performance distributed processing
- **Email Notifications**: Automatic notifications for task processing events
- **Resource Management**: Dynamic CPU and memory optimization
- **Comprehensive Monitoring**: Real-time metrics and performance tracking

## Overview

This project demonstrates:
- Creating Ray actors (agents) that maintain state
- Processing tasks asynchronously across distributed workers
- Multi-agent coordination systems
- Distributed task queues with priority scheduling
- **NEW**: SMTP-based email notifications
- **NEW**: Optimized resource utilization
- **NEW**: Production-ready error handling and retry logic

## Installation

### Quick Setup

```bash
# Clone the repository
git clone https://github.com/ibxibx/ray-agent-demo.git
cd ray-agent-demo

# Run the setup script
python setup.py

# Configure SMTP credentials
# Edit the .env file with your email credentials
```

### Manual Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env file for SMTP credentials
cp .env.template .env
# Edit .env with your SMTP settings
```

## Quick Start

### 1. Send a Test Email

```python
from ray_agent_demo import send_test_email

result = send_test_email()
print(f"Email sent: {result['result']['success']}")
```

### 2. Run the Optimized Demo

```bash
python run_optimized.py
```

Select from the menu:
- Test SMTP Agent
- Send custom email
- Run full optimized demo
- Run complete test suite

## Project Structure

```
ray-agent-demo/
├── ray_agent_demo/
│   ├── __init__.py              # Package initialization
│   ├── basic_agent.py           # Basic Ray actor demonstration
│   ├── improved_agent.py        # Enhanced agent with monitoring
│   ├── multi_agent_coordinator.py # Multi-agent coordination
│   ├── distributed_task_queue.py # Priority task queue
│   ├── smtp_agent.py            # SMTP email agent (NEW)
│   └── optimized_agent.py       # Optimized system (NEW)
├── docs/
│   ├── smtp_agent.md            # SMTP agent documentation
│   ├── installation.md          # Installation guide
│   └── quickstart.md            # Quick start guide
├── examples/
│   ├── basic_usage.py           # Basic examples
│   └── run_all_demos.py        # Run all demonstrations
├── .env                         # SMTP credentials (create from template)
├── requirements.txt             # Project dependencies
├── setup.py                     # Setup and installation script
├── run_optimized.py             # Run optimized demo with SMTP
└── README.md                    # This file
```

## Examples

### SMTP Agent Usage

```python
import ray
from ray_agent_demo import SMTPAgent

# Initialize Ray and create SMTP agent
ray.init()
agent = SMTPAgent.remote()

# Send an email
result = ray.get(agent.send_email.remote(
    to="recipient@example.com",
    subject="Hello from Ray",
    body="This is an automated email from the Ray SMTP Agent"
))

# Check result
if result["result"]["success"]:
    print(f"Email sent! ID: {result['result']['message_id']}")
```

### Optimized Agent System

```python
from ray_agent_demo import OptimizedAgentSystem, SystemConfig
import asyncio

async def main():
    # Create system with email notifications
    config = SystemConfig(
        num_cpus=8,
        enable_email_notifications=True
    )
    system = OptimizedAgentSystem(config)
    
    # Process tasks with automatic notifications
    tasks = [{"type": "compute", "priority": 5} for _ in range(100)]
    results = await system.process_batch_optimized(tasks)
    
    system.shutdown()

asyncio.run(main())
```

## SMTP Configuration

Create a `.env` file in the project root:

```env
# SMTP Server Settings
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_EMAIL=your_email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_USE_TLS=true

# Default recipient
DEFAULT_RECIPIENT=office@botworld.cloud
```

### Gmail Setup
1. Enable 2-factor authentication
2. Generate an app-specific password
3. Use the app password in SMTP_PASSWORD

## Features

### Core Features
- **State Management**: Agents maintain their own state across method calls
- **Asynchronous Processing**: Submit tasks without blocking
- **Task Tracking**: Track processing status and results
- **Status Monitoring**: Real-time agent status queries

### New Optimized Features
- **SMTP Integration**: Send emails via distributed agents
- **Connection Pooling**: Efficient SMTP connection management
- **Retry Logic**: Automatic retry with exponential backoff
- **Rate Limiting**: Prevent spam and server overload
- **Batch Processing**: Process multiple emails efficiently
- **Resource Optimization**: Dynamic CPU and memory allocation
- **Email Notifications**: Automatic progress and error notifications
- **Comprehensive Metrics**: Track performance and success rates

## Performance

The optimized system achieves:
- 100+ tasks/second throughput
- Automatic load balancing across CPUs
- Memory-efficient processing
- Real-time progress tracking
- Email notifications for milestones

## Requirements

- Python >= 3.8
- Ray >= 2.0.0
- NumPy >= 1.19.0
- Pandas >= 1.2.0
- dataclasses-json >= 0.5.7
- python-dotenv >= 0.19.0
- psutil >= 5.8.0

## Documentation

- [SMTP Agent Guide](docs/smtp_agent.md) - Detailed SMTP agent documentation
- [Installation Guide](docs/installation.md) - Setup instructions
- [Quick Start](docs/quickstart.md) - Getting started quickly

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details
