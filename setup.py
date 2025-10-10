from setuptools import setup, find_packages
import os

# Read version from version.py without importing the package
def get_version():
    version_file = os.path.join(os.path.dirname(__file__), 'pepi', 'version.py')
    with open(version_file, 'r') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"\'')
    return "1.0.0"  # fallback

setup(
    name="pepi",
    version=get_version(),
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
        "requests>=2.28.0",
        "packaging>=21.0",
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