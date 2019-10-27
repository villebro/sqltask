import io

from setuptools import find_packages, setup

from sqltask import __version__

with io.open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='sqltask',
    version=__version__,
    description="ETL tool based on SqlAlchemy for building robust ETL pipelies with "
                "high emphasis on high data quality",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/villebro/sqltask',
    author='Ville Brofeldt',
    author_email='villebro@apache.org',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
    ],
    license='MIT',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
    ],
    extras_require={
        "bigquery": ["pybigquery>=0.4.11"],
        "postgres": ["psycopg2>=2.8.3"],
        "mssql": ["pymssql>=2.1.4"],
        "snowflake": ["snowflake-sqlalchemy>=1.1.14"],
    },
)
