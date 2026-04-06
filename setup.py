"""
通达信行情数据自研接口
"""

from setuptools import setup, find_packages

setup(
    name="tdxapi",
    version="0.1.0",
    description="通达信行情数据自研接口，不依赖客户端",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[],
    extras_require={
        "dev": ["pytest>=7.0"],
    },
)
