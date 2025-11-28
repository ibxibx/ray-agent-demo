#!/usr/bin/env python3
"""
Migration script to restructure the ray-agent-demo project.
Run this script in your project root directory.
"""

import os
import shutil
from pathlib import Path

def create_directory_structure():
    """Create the new directory structure."""
    directories = [
        "ray_agent_demo",
        "examples",
        "tests",
        "docs",
        "docs/api",
        "docs/examples",
        "scripts",
        ".github",
        ".github/workflows",
        ".github/ISSUE_TEMPLATE"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")

def move_files():
    """Move files to their new locations."""
    file_mappings = {
        # Core modules to package directory
        "basic_agent.py": "ray_agent_demo/basic_agent.py",
        "multi_agent_coordinator.py": "ray_agent_demo/multi_agent_coordinator.py",
        "agent_communication.py": "ray_agent_demo/agent_communication.py",
        "distributed_task_queue.py": "ray_agent_demo/distributed_task_queue.py",
        "improved_agent.py": "ray_agent_demo/improved_agent.py",
        "__init__.py": "ray_agent_demo/__init__.py",
        
        # Examples
        "run_all_demos.py": "examples/run_all_demos.py",
        
        # Tests
        "test_agents.py": "tests/test_agents.py",
    }
    
    for source, destination in file_mappings.items():
        if os.path.exists(source):
            # Create destination directory if needed
            dest_dir = os.path.dirname(destination)
            if dest_dir:
                Path(dest_dir).mkdir(parents=True, exist_ok=True)
            
            # Move file
            shutil.move(source, destination)
            print(f"✓ Moved {source} → {destination}")
        else:
            print(f"⚠ File not found: {source}")

def create_init_files():
    """Create __init__.py files for packages."""
    init_files = {
        "examples/__init__.py": '"""Example scripts demonstrating ray-agent-demo usage."""',
        "tests/__init__.py": '"""Test suite for ray-agent-demo."""',
        "scripts/__init__.py": '"""Utility scripts for ray-agent-demo."""',
    }
    
    for filepath, content in init_files.items():
        with open(filepath, "w") as f:
            f.write(content)
        print(f"✓ Created {filepath}")

def update_imports_in_file(filepath, old_imports, new_imports):
    """Update imports in a Python file."""
    try:
        with open(filepath, "r") as f:
            content = f.read()
        
        original_content = content
        for old, new in zip(old_imports, new_imports):
            content = content.replace(old, new)
        
        if content != original_content:
            with open(filepath, "w") as f:
                f.write(content)
            print(f"✓ Updated imports in {filepath}")
        
    except Exception as e:
        print(f"⚠ Error updating {filepath}: {e}")

def update_all_imports():
    """Update imports in all files."""
    # Update imports in examples/run_all_demos.py
    if os.path.exists("examples/run_all_demos.py"):
        old_imports = [
            "from basic_agent import",
            "from multi_agent_coordinator import",
            "from agent_communication import",
            "from distributed_task_queue import",
            "from improved_agent import"
        ]
        new_imports = [
            "from ray_agent_demo.basic_agent import",
            "from ray_agent_demo.multi_agent_coordinator import",
            "from ray_agent_demo.agent_communication import",
            "from ray_agent_demo.distributed_task_queue import",
            "from ray_agent_demo.improved_agent import"
        ]
        update_imports_in_file("examples/run_all_demos.py", old_imports, new_imports)
    
    # Update imports in tests/test_agents.py
    if os.path.exists("tests/test_agents.py"):
        old_imports = [
            "from basic_agent import",
            "from multi_agent_coordinator import",
            "from agent_communication import",
            "from distributed_task_queue import",
            "from improved_agent import"
        ]
        new_imports = [
            "from ray_agent_demo.basic_agent import",
            "from ray_agent_demo.multi_agent_coordinator import",
            "from ray_agent_demo.agent_communication import",
            "from ray_agent_demo.distributed_task_queue import",
            "from ray_agent_demo.improved_agent import"
        ]
        update_imports_in_file("tests/test_agents.py", old_imports, new_imports)

def create_example_files():
    """Create additional example files."""
    # Basic usage example
    basic_usage = '''"""Basic usage examples for ray-agent-demo."""

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
    
    print("\\n=== Multi-Agent Example ===")
    multi_agent_example()
'''
    
    with open("examples/basic_usage.py", "w") as f:
        f.write(basic_usage)
    print("✓ Created examples/basic_usage.py")

def create_documentation_files():
    """Create basic documentation files."""
    # Installation guide
    installation_md = """# Installation Guide

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
"""
    
    # Quick start guide
    quickstart_md = """# Quick Start Guide

## Basic Usage

### 1. Import and Initialize

```python
import ray
from ray_agent_demo import BasicAgent

# Initialize Ray
ray.init()
```

### 2. Create an Agent

```python
# Create a basic agent
agent = BasicAgent.remote("my-agent")

# Process a task
result = ray.get(agent.process_task.remote("Hello, World!"))
```

### 3. Multi-Agent Coordination

```python
from ray_agent_demo import AgentCoordinator

# Create coordinator with 4 agents
coordinator = AgentCoordinator.remote(num_agents=4)

# Process multiple tasks
tasks = ["task1", "task2", "task3", "task4"]
results = ray.get(coordinator.process_all_tasks.remote(tasks))
```

## Next Steps

- Check out the [examples](../examples/) directory
- Read the [API documentation](api/)
- Try the advanced features in the [GUIDE](../GUIDE.md)
"""
    
    with open("docs/installation.md", "w") as f:
        f.write(installation_md)
    print("✓ Created docs/installation.md")
    
    with open("docs/quickstart.md", "w") as f:
        f.write(quickstart_md)
    print("✓ Created docs/quickstart.md")

def create_github_workflows():
    """Create GitHub Actions workflows."""
    # Test workflow
    test_workflow = """name: Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, "3.10", "3.11"]

    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]
    
    - name: Run tests
      run: |
        pytest tests/ -v
    
    - name: Run linting
      run: |
        flake8 ray_agent_demo/ --max-line-length=100
"""
    
    with open(".github/workflows/tests.yml", "w") as f:
        f.write(test_workflow)
    print("✓ Created .github/workflows/tests.yml")

def main():
    """Main migration function."""
    print("🚀 Starting ray-agent-demo project migration...")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists("setup.py"):
        print("❌ Error: setup.py not found. Are you in the project root directory?")
        return
    
    # Create directory structure
    print("\n📁 Creating directory structure...")
    create_directory_structure()
    
    # Move files
    print("\n📦 Moving files to new locations...")
    move_files()
    
    # Create init files
    print("\n📄 Creating __init__.py files...")
    create_init_files()
    
    # Update imports
    print("\n🔧 Updating imports...")
    update_all_imports()
    
    # Create example files
    print("\n📝 Creating example files...")
    create_example_files()
    
    # Create documentation
    print("\n📚 Creating documentation files...")
    create_documentation_files()
    
    # Create GitHub workflows
    print("\n⚙️ Creating GitHub workflows...")
    create_github_workflows()
    
    print("\n✅ Migration completed successfully!")
    print("\n📋 Next steps:")
    print("1. Review the new structure")
    print("2. Test the package: pip install -e .")
    print("3. Run tests: pytest tests/")
    print("4. Try examples: python examples/basic_usage.py")
    print("5. Commit changes: git add . && git commit -m 'Restructure project'")

if __name__ == "__main__":
    main()
