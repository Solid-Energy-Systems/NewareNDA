'''
用于将 Neware NDA 文件转换为其他文件格式的脚本。默认
输出格式为 csv。其他格式可能需要安装额外的包。
'''
import argparse
from logging import _nameToLevel
import pandas as pd
import NewareNDA


def main():
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

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('in_file', help='输入文件')
    parser.add_argument('out_file', help='输出文件')
    parser.add_argument('-f', '--format', default='csv',
                        choices=output_cmd.keys())
    parser.add_argument('-s', '--no_software_cycle_number', action='store_false',
                        help='生成循环编号字段以匹配旧版本的 BTSDA。')
    parser.add_argument('-v', '--version', help='显示版本',
                        action='version', version=NewareNDA.__version__)
    parser.add_argument('-l', '--log_level', choices=list(_nameToLevel.keys()), default='INFO',
                        help='设置 NewareNDA 的日志级别')
    parser.add_argument('-c', '--cycle_mode', choices=['chg', 'dchg', 'auto'], default='chg',
                        help='选择循环递增方式。')
    args = parser.parse_args()

    df = NewareNDA.read(args.in_file, args.no_software_cycle_number, cycle_mode=args.cycle_mode, log_level=args.log_level)
    output_cmd[args.format](df, args.out_file)


if __name__ == '__main__':
    main()
