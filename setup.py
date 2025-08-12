from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pgdn-sui",
    version="2.7.0",
    description="Fast, step-based blockchain validator scanner for Sui network",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="PGDN Team",
    author_email="team@pgdn.io",
    url="https://github.com/pgdn/pgdn-sui",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "click>=8.0.0",
        "urllib3>=1.26.0"
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "black>=21.0.0",
            "flake8>=3.8.0",
            "mypy>=0.800"
        ]
    },
    entry_points={
        'console_scripts': [
            'pgdn-sui=pgdn_sui.cli:cli',
            'pgdn-sui-advanced=pgdn_sui.advanced:main',
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking :: Monitoring",
    ],
)
