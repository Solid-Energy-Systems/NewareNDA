import pandas as pd
import NewareNDA


def test_NDA(nda_file, ref_file):
    df = NewareNDA.read(nda_file)
    ref_df = pd.read_feather(ref_file)
    assert df.compare(ref_df).empty
