# © 2022 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='NewareNDA',
    version='v2022.08.01',
    description='Neware nda binary file reader.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Daniel Cogswell',
    author_email='danielcogswell@ses.ai',
    url='https://github.com/d-cogswell/NewareNDA',
    py_modules=['NewareNDA'],
    install_requires=['pandas'],
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
)
