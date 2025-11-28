"""
Run all Ray Agent demonstrations
This script runs through all the agent examples in sequence.
"""

import time
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

def print_section(title):
    """Print a formatted section header."""
    print("\n" + "="*60)
    print(f" {title} ")
    print("="*60 + "\n")

def run_demo(demo_name, demo_function):
    """Run a demo with error handling."""
    try:
        print(f"Running {demo_name}...")
        demo_function()
        print(f"✓ {demo_name} completed successfully!")
        return True
    except Exception as e:
        print(f"✗ {demo_name} failed with error: {e}")
        return False

def main():
    """Run all demonstrations."""
    print_section("RAY AGENT DEMO SUITE")
    print("This will run through all the agent examples.")
    print("Each demo showcases different Ray agent patterns and capabilities.")
    
    successes = 0
    failures = 0
    
    # 1. Basic Agent Demo
    print_section("1. BASIC AGENT DEMO")
    print("Demonstrates fundamental Ray actor concepts:")
    print("- Creating Ray actors (agents)")
    print("- Processing tasks asynchronously")
    print("- Maintaining state across method calls")
    
    from ray_agent_demo.basic_agent import demo_basic_agent
    if run_demo("Basic Agent", demo_basic_agent):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    # 2. Multi-Agent Coordinator Demo
    print_section("2. MULTI-AGENT COORDINATOR")
    print("Shows how to coordinate multiple agents:")
    print("- Managing a pool of agents")
    print("- Distributing tasks across agents")
    print("- Load balancing strategies")
    
    from ray_agent_demo.multi_agent_coordinator import demo_multi_agent, demo_load_balanced
    if run_demo("Multi-Agent Coordination", demo_multi_agent):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    if run_demo("Load-Balanced Coordination", demo_load_balanced):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    # 3. Agent Communication Demo
    print_section("3. AGENT COMMUNICATION")
    print("Demonstrates inter-agent communication patterns:")
    print("- Direct agent-to-agent messaging")
    print("- Broadcasting to multiple agents")
    print("- Publish-subscribe patterns")
    
    from ray_agent_demo.agent_communication import demo_direct_communication, demo_pubsub_pattern
    if run_demo("Direct Communication", demo_direct_communication):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    if run_demo("Pub/Sub Pattern", demo_pubsub_pattern):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    # 4. Distributed Task Queue Demo
    print_section("4. DISTRIBUTED TASK QUEUE")
    print("Implements a practical task queue system:")
    print("- Priority-based task scheduling")
    print("- Multiple worker agents")
    print("- Task retry logic")
    print("- Performance monitoring")
    
    from ray_agent_demo.distributed_task_queue import demo_task_queue
    if run_demo("Distributed Task Queue", demo_task_queue):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    # 5. Improved Agent Demo
    print_section("5. IMPROVED AGENT WITH ADVANCED FEATURES")
    print("Enhanced agent with production-ready features:")
    print("- Async task processing")
    print("- Error handling and retry logic")
    print("- Metrics collection")
    print("- State persistence")
    print("- Agent monitoring")
    
    from ray_agent_demo.improved_agent import demo_improved_agent, demo_monitoring
    if run_demo("Improved Agent", demo_improved_agent):
        successes += 1
    else:
        failures += 1
    
    time.sleep(2)
    
    if run_demo("Agent Monitoring", demo_monitoring):
        successes += 1
    else:
        failures += 1
    
    # Summary
    print_section("DEMO SUITE SUMMARY")
    total = successes + failures
    print(f"Total demos run: {total}")
    print(f"Successful: {successes}")
    print(f"Failed: {failures}")
    
    if failures == 0:
        print("\n✓ All demos completed successfully!")
        print("\nNext steps:")
        print("1. Explore individual demo files for detailed implementations")
        print("2. Run test_agents.py to execute the test suite")
        print("3. Modify and extend the examples for your use cases")
        print("4. Check out the Ray documentation for more advanced features")
    else:
        print(f"\n⚠ {failures} demo(s) failed. Please check the error messages above.")
    
    print("\nThank you for exploring the Ray Agent Demo!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDemo suite interrupted by user.")
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
    finally:
        # Ensure Ray is shutdown
        import ray
        if ray.is_initialized():
            ray.shutdown()
