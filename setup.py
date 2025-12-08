from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="database-expectations",
    version="0.1.0",
    author="Thor011",
    author_email="",
    description="Production-ready Great Expectations toolkit for database testing and data quality validation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Thor011/database-expectations-python",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Testing",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "great-expectations>=0.18.0",
        "sqlalchemy>=2.0.0",
        "pandas>=1.3.0",  # Compatible with Python 3.8
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0.0",
        "click>=8.1.0",
    ],
    extras_require={
        "dev": ["pytest>=7.4.0", "pytest-cov>=4.1.0"],
        "postgres": ["psycopg2-binary>=2.9.0"],
        "mysql": ["pymysql>=1.1.0"],
        "sqlserver": ["pyodbc>=5.0.0"],
        "oracle": ["cx_Oracle>=8.3.0"],
        "scheduler": ["apscheduler>=3.10.0"],
        "all": [
            "psycopg2-binary>=2.9.0",
            "pymysql>=1.1.0",
            "pyodbc>=5.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "db-expect=db_expectations.cli:main",
        ],
    },
)
