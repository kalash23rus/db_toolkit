from setuptools import setup, find_packages

from db_tools import __version__

setup(
    name='db_tools',
    version=__version__,

    url='https://gitlab.com/Kalashnikov_A/kalashnikov_toolkit/-/blob/sandbox',
    author='kalash_23rus',
    author_email='a.kalashnikov@insilicomedicine.com',

    packages=find_packages(),
    install_requires=[
            'psycopg2-binary==2.9.3',
            'pandas>=1.0',
            'requests>=2.27.1',
            'neo4j'
            ],
)
