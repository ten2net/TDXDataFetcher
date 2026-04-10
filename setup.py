"""
TDX Market Data API - Direct TCP connection to TDX servers
"""

from setuptools import setup, find_packages

setup(
    name="tdxapi",
    version="0.1.0",
    description="TDX Market Data API - Direct TCP connection without client software",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[],
    extras_require={
        "dev": ["pytest>=7.0"],
        "compression": ["lz4>=4.0"],
        "pandas": ["pandas>=1.3.0"],
        "polars": ["polars>=0.18.0"],
        "all": ["pandas>=1.3.0", "polars>=0.18.0", "lz4>=4.0"],
    },
)
