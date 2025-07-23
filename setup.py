from setuptools import setup, find_packages

setup(
    name="pepi",
    version="0.0.1.6",
    description="A fast, user-friendly MongoDB log analysis tool",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/jenunes/pepi",
    py_modules=["pepi"],
    install_requires=[
        "click",
        "PyYAML", 
        "tqdm",
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