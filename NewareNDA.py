# © 2022 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import mmap
import struct
import logging
from datetime import datetime
import pandas as pd


def read(file):
    '''
    Function read electrochemical data from a Neware nda binary file.

    Args:
        file (str): Name of a .nda file to read
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    '''
    with open(file, "r+b") as f:
        mm = mmap.mmap(f.fileno(), 0)

        if mm.read(6) != b'NEWARE':
            raise Exception(f"{file} does not appear to be a Neware file.")

        # Try to find server and client version info
        version_loc = mm.find(b'BTSServer')
        if version_loc != -1:
            mm.seek(version_loc)
            server = mm.read(50).strip(b'\x00').decode()
            logging.info(f"Server: {server}")
            mm.seek(50, 1)
            client = mm.read(50).strip(b'\x00').decode()
            logging.info(f"Client: {client}")
        else:
            logging.info("File version not found!")

        # Identify the beginning of the data section
        record_len = 86
        header = mm.find(b'\x00\x00\x00\x00\x55\x00') + 4
        while mm[header + record_len] != 85:
            header = mm.find(b'\x00\x00\x00\x00\x55\x00', header) + 4
        mm.seek(header)

        # Read data records
        output = []
        while mm.tell() < mm.size():
            bytes = mm.read(record_len)
            if len(bytes) == record_len and bytes[0] == 85:
                output.append(bytes_to_df(bytes))

    # Create DataFrame
    df = pd.DataFrame(output)
    df.set_index('Index', inplace=True)

    # Postprocessing
    df.Step = count_changes(df.Step)
    df.Cycle = generate_cycle_number(df)

    # Define precision of fields
    dtype_dict = {
        'Cycle': 'int16',
        'Step': 'int16',
        'Time': 'float32',
        'Voltage': 'float32',
        'Current(mA)': 'float32',
        'Capacity(mAh)': 'float32',
        'Energy(mWh)': 'float32'
    }
    df = df.astype(dtype=dtype_dict)

    return(df)


def bytes_to_df(bytes):
    '''
    Helper function for interpreting a byte string
    '''
    # Dictionary mapping Status integer to string
    state_dict = {
        1: 'CC_Chg',
        2: 'CC_DChg',
        4: 'Rest',
        7: 'CCCV_Chg',
        13: 'Pause',
        19: 'CV_DChg',
        20: 'CCCV_DChg'
    }

    # Extract fields from byte string
    [Index, Cycle] = struct.unpack('<IB', bytes[2:7])
    [Step, Status, Jump, Time] = struct.unpack('<HBBQ', bytes[10:22])
    [Voltage, Current] = struct.unpack('<ii', bytes[22:30])
    [Charge_capacity, Discharge_capacity] = struct.unpack('<qq', bytes[38:54])
    [Charge_energy, Discharge_energy] = struct.unpack('<qq', bytes[54:70])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', bytes[70:77])
    [Range] = struct.unpack('<i', bytes[78:82])

    # Convert date to datetime. Try Unix timestamp on failure.
    try:
        Date = datetime(Y, M, D, h, m, s)
    except ValueError:
        [Timestamp] = struct.unpack('<Q', bytes[70:78])
        Date = datetime.fromtimestamp(Timestamp)

    # Define field scaling based on instrument Range setting
    multiplier_dict = {
        -20000: 1e-2,
        -3000: 1e-2,
        -100: 1e-3,
        0: 0,
        10: 1e-3,
        100: 1e-2,
        200: 1e-2,
        1000: 1e-1,
        6000: 1e-1,
        12000: 1e-1
    }
    multiplier = multiplier_dict[Range]

    # Create a dictionary for the record
    rec = {
        'Index': Index,
        'Cycle': Cycle + 1,
        'Step': Step,
        'Status': state_dict[Status],
        'Time': Time/1000,
        'Voltage': Voltage/10000,
        'Current(mA)': Current*multiplier,
        'Capacity(mAh)': (Charge_capacity+Discharge_capacity)*multiplier/3600,
        'Energy(mWh)': (Charge_energy+Discharge_energy)*multiplier/3600,
        'Timestamp': Date
    }
    return(rec)


def generate_cycle_number(df):
    '''
    Generate the cycle number by incrementing at the beginning each charge step
    '''
    chg = (df.Status == 'CCCV_Chg') | (df.Status == 'CC_Chg')
    chg = (chg - chg.shift()).clip(0)
    chg.iat[0] = 0
    cycle = chg.cumsum()
    cycle.loc[cycle == 0] = 1
    return(cycle)


def count_changes(series):
    '''
    Enumerate the number of value changes in a series
    '''
    a = series.diff()
    a.iloc[0] = 1
    a.iloc[-1] = 0
    return((abs(a) > 0).cumsum())
