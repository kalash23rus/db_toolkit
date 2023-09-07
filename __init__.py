from setuptools import setup, find_packages

# Read the contents of the requirements.txt file
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

# Read the version directly from the __init__.py file
with open('db_tools/__init__.py') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.split('=')[1].strip().strip("'")
            break

setup(
    name='db_tools',
    version=version,

    url='https://gitlab.com/Kalashnikov_A/kalashnikov_toolkit/-/blob/sandbox',
    author='kalash_23rus',
    author_email='a.kalashnikov@insilicomedicine.com',

    packages=find_packages(),
    install_requires=requirements,
)
