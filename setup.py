from setuptools import find_packages, setup

setup(
    name='sqltask',
    version='0.1.4',
    description='ETL tool for performing mostly SQL-based data transformation',
    long_description='',
    url='https://github.com/villebro/sqltask',
    author='Ville Brofeldt',
    author_email='villebro@apache.org',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'sqlalchemy-utils',
    ],
    license='MIT',
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
    ],
    extras_require={
        "bigquery": ["pybigquery>=0.4.11"],
        "postgres": ["psycopg2>=2.8.3"],
        "mssql": ´["pymssql>=2.1.4"],
        "snowflake": ["snowflake-sqlalchemy>=1.1.14"],
    },
)
