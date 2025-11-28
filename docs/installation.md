# Installation Guide

## Requirements

- Python 3.8 or higher
- Ray 2.9.0 or higher

## Installation Options

### 1. Install from source (recommended for development)

```bash
git clone https://github.com/ibxibx/ray-agent-demo.git
cd ray-agent-demo
pip install -e .
```

### 2. Install requirements only

```bash
pip install -r requirements.txt
```

### 3. Install with development dependencies

```bash
pip install -e .[dev]
```

## Verify Installation

```python
import ray
from ray_agent_demo import BasicAgent

ray.init()
agent = BasicAgent.remote("test")
print("Installation successful!")
ray.shutdown()
```
