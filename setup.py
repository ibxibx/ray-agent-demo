from setuptools import setup, find_packages

setup(
    name="ray-agent-demo",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A basic Ray agent demonstration for learning distributed computing",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ibxibx/ray-agent-demo",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    install_requires=[
        "ray[default]>=2.9.0",
        "numpy>=1.24.0",
        "pandas>=2.0.0",
    ],
)