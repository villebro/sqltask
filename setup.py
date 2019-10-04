from setuptools import find_packages, setup

setup(
    name='sqltask',
    version='0.0.5',
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
        "snowflake": ["snowflake-sqlalchemy>=1.1.14"],
    },
)
