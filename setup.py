from setuptools import setup, find_packages

from db_tools import __version__

# Read the contents of the requirements.txt file
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name='db_tools',
    version=__version__,

    url='https://gitlab.com/Kalashnikov_A/kalashnikov_toolkit/-/blob/sandbox',
    author='kalash_23rus',
    author_email='a.kalashnikov@insilicomedicine.com',

    packages=find_packages(),
    install_requires=requirements,
)
