# © 2023 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import setuptools

version = {}
with open('NewareNDA/version.py', 'r', encoding='utf-8') as fh:
    exec(fh.read(), version)
__version__ = version['__version__']

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='NewareNDA',
    version=__version__,
    description='Neware nda binary file reader.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Daniel Cogswell',
    author_email='danielcogswell@ses.ai',
    url='https://github.com/Solid-Energy-Systems/NewareNDA',
    license='BSD-3-Clause',
    packages=['NewareNDA'],
    scripts=['bin/NewareNDA-cli.py'],
    install_requires=['pandas'],
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
