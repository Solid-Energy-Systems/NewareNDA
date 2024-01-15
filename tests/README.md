# Regression tests
This tests folder has a regression test that compares reads a directory of nda files and compares the results to a set of reference dataframes stored as feather files. The comparison is performed using `pandas.testing.assert_frame_equal()`. 

## Usage
After installing `pytest`, run from the root directory of NewareNDA:

```pytest --ndaDir=tests/nda --refDir=tests/reference tests```
