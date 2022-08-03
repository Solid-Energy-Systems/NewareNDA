# NewareNDA

© 2022 Copyright SES AI
<br>Author: Daniel Cogswell
<br>Email: danielcogswell@ses.ai

Neware NDA binary file reader.

# Installation
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
NewareNDA in_file.nda --format feather out_file.ftr
```
The following `--format` options are supported: `csv, excel, feather, hdf, json, parquet, pickle, stata`
