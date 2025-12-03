from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ray-agent-demo",
    version="2.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="High-performance Ray agent demonstration with optimized distributed computing patterns",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ibxibx/ray-agent-demo",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "ray[default]>=2.9.0",
        "numpy>=1.24.0",
    ],
    entry_points={
        "console_scripts": [
            "ray-agent-demo=run_optimized_demo:run_quick_demo",
            "ray-agent-benchmark=run_optimized_demo:run_performance_benchmark",
        ],
    },
    keywords="ray distributed-computing agents actors parallel-processing high-performance",
    project_urls={
        "Bug Reports": "https://github.com/ibxibx/ray-agent-demo/issues",
        "Source": "https://github.com/ibxibx/ray-agent-demo",
    },
)