from setuptools import setup

with open('README.md', 'r') as f:
    data = f.read()
setup(
    name="pysparkutils", 
    version=0.1, 
    description="Code Snippets for PySpark", 
    long_description=data, 
    author="Narendra",
    packages=["pysparkutils"],
    author_email="narendrababuoggu393@gmail.com",
    install_requires=['pyspark==2.4.7', 'pandas', 'pytest', 'pyhocon'],
)
