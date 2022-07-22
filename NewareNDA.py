# © 2022 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import mmap
import struct
import logging
from datetime import datetime
import pandas as pd


def read(file, start_index=None):
    '''
    Function read electrochemical data from a Neware nda binary file.

    Args:
        file (str): Name of a .nda file to read
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    '''
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

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
        identifier = b'\x00\x00\x00\x00\x55\x00'
        header = mm.find(identifier)
        if header == -1:
            raise EOFError(f"File {file} does not contain any valid records.")
        while (mm[header + 4 + record_len] != 85 if header + 4 + record_len < mm.size()
               else False):
            header = mm.find(identifier, header)
        mm.seek(header + 4)

        # Optionally find start_index
        if isinstance(start_index, int):
            mm.seek(-4, 1)
            header = mm.find(identifier + start_index.to_bytes(4, byteorder='little'))
            if header == -1:
                raise EOFError(f"File {file} does not contain any valid records.")
            mm.seek(header + 4)

        # Read data records
        output = []
        aux = []
        while mm.tell() < mm.size():
            bytes = mm.read(record_len)
            if len(bytes) == record_len:
                if bytes[0:1] == b'\x55':
                    output.append(_bytes_to_dict(bytes))
                elif bytes[0:1] == b'\x65':
                    aux.append(_aux_bytes_to_dict(bytes))

    # Create DataFrame
    df = pd.DataFrame(output)
    df.set_index('Index', inplace=True)

    # Join temperature data
    aux_df = pd.DataFrame(aux)
    if not aux_df.empty:
        aux_df.set_index('Index', inplace=True)
        for Aux in aux_df.Aux.unique():
            df = df.join(aux_df.loc[aux_df.Aux == Aux, 'T'])
            df.rename(columns={'T': f"T{Aux}"}, inplace=True)

    # Postprocessing
    df.Step = _count_changes(df.Step)
    df.Cycle = _generate_cycle_number(df)

    # Define precision of fields
    dtype_dict = {
        'Cycle': 'int16',
        'Step': 'int16',
        'Jump': 'int16',
        'Time': 'float32',
        'Voltage': 'float32',
        'Current(mA)': 'float32',
        'Charge_Capacity(mAh)': 'float32',
        'Discharge_Capacity(mAh)': 'float32',
        'Charge_Energy(mWh)': 'float32',
        'Discharge_Energy(mWh)': 'float32'
    }
    df = df.astype(dtype=dtype_dict)

    return(df)


def _bytes_to_dict(bytes):
    '''
    Helper function for interpreting a byte string
    '''
    # Dictionary mapping Status integer to string
    state_dict = {
        1: 'CC_Chg',
        2: 'CC_DChg',
        3: 'CV_Chg',
        4: 'Rest',
        5: 'Cycle',
        7: 'CCCV_Chg',
        13: 'Pause',
        17: 'SIM',
        19: 'CV_DChg',
        20: 'CCCV_DChg'
    }

    # Extract fields from byte string
    [Index, Cycle] = struct.unpack('<IB', bytes[2:7])
    [Step] = struct.unpack('<I', bytes[10:14])
    [Status, Jump, Time] = struct.unpack('<BBQ', bytes[12:22])
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
        -100000: 1e-2,
        -60000: 1e-2,
        -50000: 1e-2,
        -20000: 1e-2,
        -6000: 1e-2,
        -3000: 1e-2,
        -100: 1e-3,
        0: 0,
        10: 1e-3,
        100: 1e-2,
        200: 1e-2,
        1000: 1e-1,
        6000: 1e-1,
        12000: 1e-1,
        50000: 1e-1,
    }
    multiplier = multiplier_dict[Range]

    # Create a dictionary for the record
    dict = {
        'Index': Index,
        'Cycle': Cycle + 1,
        'Step': Step,
        'Status': state_dict[Status],
        'Jump': Jump,
        'Time': Time/1000,
        'Voltage': Voltage/10000,
        'Current(mA)': Current*multiplier,
        'Charge_Capacity(mAh)': Charge_capacity*multiplier/3600,
        'Discharge_Capacity(mAh)': Discharge_capacity*multiplier/3600,
        'Charge_Energy(mWh)': Charge_energy*multiplier/3600,
        'Discharge_Energy(mWh)': Discharge_energy*multiplier/3600,
        'Timestamp': Date
    }
    return(dict)


def _aux_bytes_to_dict(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack('<B', bytes[1:2])
    [Index, Cycle] = struct.unpack('<IB', bytes[2:7])
    [T] = struct.unpack('<h', bytes[34:36])

    dict = {
        'Index': Index,
        'Aux': Aux,
        'T': T/10
    }

    return(dict)


def _generate_cycle_number(df):
    '''
    Generate a cycle number to match Neware. A new cycle starts with a charge
    step after there has previously been a discharge.
    '''

    # Identify the beginning of charge steps
    chg = (df.Status == 'CCCV_Chg') | (df.Status == 'CC_Chg')
    chg = (chg - chg.shift()).clip(0)
    chg.iat[0] = 1

    # Convert to numpy arrays
    chg = chg.values
    status = df.Status.values

    # Increment the cycle at a charge step after there has been a discharge
    cyc = 1
    dchg = False
    for n in range(len(chg)):
        if chg[n] & dchg:
            cyc += 1
            dchg = False
        elif 'DChg' in status[n] or status[n] == 'SIM':
            dchg = True
        chg[n] = cyc

    return(chg)


def _count_changes(series):
    '''
    Enumerate the number of value changes in a series
    '''
    a = series.diff()
    a.iloc[0] = 1
    return((abs(a) > 0).cumsum())
