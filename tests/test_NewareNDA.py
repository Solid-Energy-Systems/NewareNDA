import pandas as pd
import NewareNDA


def test_NDA(nda_file, ref_file):
    df = NewareNDA.read(nda_file)
    ref_df = pd.read_feather(ref_file)
    pd.testing.assert_frame_equal(df, ref_df, check_like=True)
