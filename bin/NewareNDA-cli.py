#!/usr/bin/env python3
'''
Script for converting Neware NDA files to other file formats. The default
output format is csv. Other formats may require installing additional packages.
'''
import argparse
import pandas as pd
import NewareNDA

output_cmd = {
    'csv': lambda df, f: pd.DataFrame.to_csv(df, f, index=False),
    'excel': lambda df, f: pd.DataFrame.to_excel(df, f, index=False),
    'feather': pd.DataFrame.to_feather,
    'hdf': lambda df, f: pd.DataFrame.to_hdf(df, f, key='Index'),
    'json': pd.DataFrame.to_json,
    'parquet': pd.DataFrame.to_parquet,
    'pickle': pd.DataFrame.to_pickle,
    'stata': pd.DataFrame.to_stata
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('in_file', help='input file')
    parser.add_argument('out_file', help='output file')
    parser.add_argument('-f', '--format', default='csv',
                        choices=output_cmd.keys())
    parser.add_argument('-s', '--software_cycle_number', action='store_true',
                        help='Generate the cycle number field to match old versions of BTSDA.')
    parser.add_argument('-v', '--version', help='show version',
                        action='version', version=NewareNDA.__version__)
    args = parser.parse_args()

    df = NewareNDA.read(args.in_file, args.software_cycle_number)
    output_cmd[args.format](df, args.out_file)
