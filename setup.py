from setuptools import setup, find_namespace_packages

version = "1.0.0"

setup(
    name="metaflow-dbt-extension",
    version=version,
    description="DBT extension for Metaflow",
    long_description=open("README.md").read(),
    author="Sakari Ikonen",
    long_description_content_type="text/markdown",
    license="Apache Software License",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    python_requires=">=3.8",
    install_requires=["metaflow>=2.8.3", "dbt-core", "pyyaml"],
)
