from setuptools import setup, find_packages

setup(
    name="ASFIN",
    version="0.1",
    packages=find_packages(),  # Automatically detects your package folders
    install_requires=[
        "numpy",
        "pandas", 
        "scikit-learn", 
        "spacy", 
        "rapidfuzz",
    ],
)