from setuptools import setup

setup(
    name='sqltask',
    version='0.0.2',
    description='ETL tool for performing mostly SQL-based data transformation',
    long_description='',
    url='https://github.com/villebro/sqltask',
    author='Ville Brofeldt',
    author_email='villebro@apache.org',
    packages=[''],
    install_requires=[
        'sqlalchemy',
        'sqlalchemy-utils',
    ],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
    ],
)
