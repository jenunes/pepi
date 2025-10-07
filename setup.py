from setuptools import setup, find_packages

setup(
    name="pepi",
    version="0.0.2.2",
    description="A fast, user-friendly MongoDB log analysis tool",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/jenunes/pepi",
    packages=find_packages(),
    package_data={
        "pepi": ["web_static/*", "web_static/**/*"],
    },
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "PyYAML>=6.0", 
        "tqdm>=4.62.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.23.0",
        "python-multipart>=0.0.6",
        "psutil>=5.9.0",
        "python-dateutil>=2.8.0",
    ],
    entry_points={
        "console_scripts": [
            "pepi=pepi:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.8",
) 