import pandas as pd
import NewareNDA
from datetime import datetime


def test_NDA(nda_file, ref_file):
    df = NewareNDA.read(nda_file)
    ref_df = pd.read_feather(ref_file)

    # Convert dates to timestamps for comparison
    df['Timestamp'] = df['Timestamp'].apply(datetime.timestamp)
    ref_df['Timestamp'] = ref_df['Timestamp'].apply(datetime.timestamp)

    pd.testing.assert_frame_equal(df, ref_df, check_like=True)
