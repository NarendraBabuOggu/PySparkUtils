from setuptools import setup

setup(
    name="pysparkutils", 
    version=0.1, 
    description="Code Snippets for PySpark", 
    author="Narendra",
    packages=["pysparkutils"],
    author_email="narendrababuoggu393@gmail.com",
    install_requires=['pyspark==2.4.7', 'pandas', 'pytest'],
)
