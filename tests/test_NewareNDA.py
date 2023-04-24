import os
import glob
import pandas as pd
import NewareNDA

import pytest

nda_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nda')
ref_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reference')

# Generate list of files to compare
nda_files = glob.glob(nda_dir + '/**/*.nda', recursive=True)
ref_files = [os.path.join(ref_dir, f"{os.path.splitext(os.path.basename(f))[0]}.ftr") for f in nda_files]
testdata = list(zip(nda_files, ref_files))


@pytest.mark.parametrize("nda_file, ref_file", testdata)
def test_NDA(nda_file, ref_file):
    df = NewareNDA.read(nda_file)
    ref_df = pd.read_feather(ref_file)
    assert df.compare(ref_df).empty
