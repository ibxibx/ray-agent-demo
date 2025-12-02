from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ray-agent-demo",
    version="0.2.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A comprehensive Ray agent demonstration showcasing distributed computing patterns",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ibxibx/ray-agent-demo",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
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
        "pandas>=2.0.0",
        "dataclasses-json>=0.6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "ray-agent-demo=run_all_demos:main",
        ],
    },
    keywords="ray distributed-computing agents actors parallel-processing",
    project_urls={
        "Bug Reports": "https://github.com/ibxibx/ray-agent-demo/issues",
        "Source": "https://github.com/ibxibx/ray-agent-demo",
    },
)