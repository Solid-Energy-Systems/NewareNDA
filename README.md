[![release](https://img.shields.io/github/v/release/Solid-Energy-Systems/NewareNDA)](https://github.com/Solid-Energy-Systems/NewareNDA/releases)
[![NewareNDA regression tests](https://github.com/Solid-Energy-Systems/NewareNDA/actions/workflows/NewareNDA_pytest.yml/badge.svg)](https://github.com/Solid-Energy-Systems/NewareNDA/actions/workflows/NewareNDA_pytest.yml)
[![Coverage Status](https://coveralls.io/repos/github/Solid-Energy-Systems/NewareNDA/badge.svg?branch=development)](https://coveralls.io/github/Solid-Energy-Systems/NewareNDA?branch=development)
# NewareNDA

© 2022-2024 Copyright SES AI
<br>Author: Daniel Cogswell
<br>Email: danielcogswell@ses.ai

Python module and command line tool for reading and converting Neware nda and ndax battery cycling files. Auxiliary temperature fields are currently supported in both formats.

# Installation
To install the latest version from the PyPi package repository:
```
pip install --upgrade NewareNDA
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
df = NewareNDA.read('filename.nda(x)')
```

If you are only dealing with ndax files you can also import an NDAx class, and get access to metadata:

```
from NewareNDA.NewareNDAx import NDAx

ndax = NDAx('filename.ndax')
ndax_file.read_ndax()
data = ndax_file.data_df
# Test parameters like ndax_file.{barcode,active_mass,PN,start_time} can now be accessed,
# and test equipment parameters like ndax_file.{server,client,control_unit,tester}_version
```

## Logging
Additional test information including active mass, remarks, and BTS versions is returned via [logging](https://docs.python.org/3/library/logging.html). The following command will print this logging information to the terminal:
```
import logging
logging.basicConfig()
```

## Command-line interface:
```
usage: NewareNDA-cli [-h]
                     [-f {csv,excel,feather,hdf,json,parquet,pickle,stata}]
                     [-s] [-v]
                     [-l {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}]
                     [-c {chg,dchg,auto}]
                     in_file out_file

Script for converting Neware NDA files to other file formats. The default
output format is csv. Other formats may require installing additional
packages.

positional arguments:
  in_file               input file
  out_file              output file

options:
  -h, --help            show this help message and exit
  -f {csv,excel,feather,hdf,json,parquet,pickle,stata}, --format {csv,excel,feather,hdf,json,parquet,pickle,stata}
  -s, --software_cycle_number
                        Generate the cycle number field to match old versions
                        of BTSDA.
  -v, --version         show version
  -l {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}, --log_level {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}
                        Set the logging level for NewareNDA
  -c {chg,dchg,auto}, --cycle_mode {chg,dchg,auto}
                        Selects how the cycle is incremented.
```

# Troubleshooting
If you encounter a key error, it is often the case that your file has a hardware setting that we have not seen before. Usually it is a quick fix that requires comparing output from BTSDA with values extracted by NewareNDA. Please start a new Github Issue and we will help debug.
