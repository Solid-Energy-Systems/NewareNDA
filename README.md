[![release](https://img.shields.io/github/v/release/Solid-Energy-Systems/NewareNDA)](https://github.com/Solid-Energy-Systems/NewareNDA/releases)
[![NewareNDA regression tests](https://github.com/Solid-Energy-Systems/NewareNDA/actions/workflows/NewareNDA_pytest.yml/badge.svg)](https://github.com/Solid-Energy-Systems/NewareNDA/actions/workflows/NewareNDA_pytest.yml)
[![Coverage Status](https://coveralls.io/repos/github/Solid-Energy-Systems/NewareNDA/badge.svg?branch=development)](https://coveralls.io/github/Solid-Energy-Systems/NewareNDA?branch=development)
# NewareNDA

© 2024 Copyright SES AI
<br>Author: Daniel Cogswell
<br>Email: danielcogswell@ses.ai

Python module and command line tool for reading and converting Neware nda and ndax battery cycling files. Auxiliary temperature fields are currently supported in both formats.

# Installation
To install from the PyPi package repository:
```
pip install NewareNDA
```

To install the development branch directly from Github:
```
pip install git+https://github.com/Solid-Energy-Systems/NewareNDA.git@development
```

To install from source, clone this repository and run:
```
cd NewareNDA
pip install .
```

# Usage
```
import NewareNDA
df = NewareNDA.read('filename.nda')
```
## Command-line interface:
```
NewareNDA-cli.py in_file.nda --format feather out_file.ftr
```
The following `--format` options are supported: `csv, excel, feather, hdf, json, parquet, pickle, stata`

# Troubleshooting
If you encounter a key error, it is often the case that your file has a hardware setting that we have not seen before. Usually it is a quick fix that requires comparing output from BTSDA with values extracted by NewareNDA. Please start a new Github Issue and we will help debug.
