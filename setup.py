from setuptools import setup, find_packages

def read_requirements():
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="brokerchooser_takehome",
    version="0.1.0",
    packages=find_packages(),
    install_requires=read_requirements(),
    author="Adam Groszeibl",
    author_email="adam.groszeibl@icloud.com.com",
    description="There are records with Peace for Russia",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)