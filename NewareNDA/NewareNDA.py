# © 2023 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import os
import mmap
import struct
import logging
from datetime import datetime
import pandas as pd

from NewareNDA.dicts import rec_columns, aux_columns, dtype_dict, \
    multiplier_dict, state_dict
from .NewareNDAx import read_ndax


def read(file, software_cycle_number=True):
    """
    Read electrochemical data from an Neware nda or ndax binary file.

    Args:
        file (str): Name of an .nda or .ndax file to read
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    _, ext = os.path.splitext(file)
    if ext == '.nda':
        return read_nda(file, software_cycle_number)
    elif ext == '.ndax':
        return read_ndax(file)
    else:
        raise TypeError("File type not supported!")


def read_nda(file, software_cycle_number):
    """
    Function read electrochemical data from a Neware nda binary file.

    Args:
        file (str): Name of a .nda file to read
        software_cycle_number (bool): Generate the cycle number field
        to match old versions of BTSDA
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        if mm.read(6) != b'NEWARE':
            raise ValueError(f"{file} does not appear to be a Neware file.")

        # Get the file version
        [nda_version] = struct.unpack('<B', mm[14:15])
        logging.info(f"NDA version: {nda_version}")

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
            logging.info("BTS version not found!")

        # Identify the beginning of the data section
        record_len = 86
        identifier = b'\x00\x00\x00\x00\x55\x00'
        header = mm.find(identifier)
        if header == -1:
            raise EOFError(f"File {file} does not contain any valid records.")
        while (((mm[header + 4 + record_len] != 85)
                | (not _valid_record(mm[header+4:header+4+record_len])))
               if header + 4 + record_len < mm_size
               else False):
            header = mm.find(identifier, header + 4)
        mm.seek(header + 4)

        # Read data records
        output = []
        aux = []
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            if len(bytes) == record_len:

                # Check for a data record
                if (bytes[0:2] == b'\x55\x00'
                        and bytes[82:87] == b'\x00\x00\x00\x00'):
                    output.append(_bytes_to_list(bytes))

                # Check for an auxiliary record
                elif (bytes[0:1] == b'\x65'
                      and bytes[82:87] == b'\x00\x00\x00\x00'):
                    aux.append(_aux_bytes_to_list(bytes))

    # Create DataFrame and sort by Index
    df = pd.DataFrame(output, columns=rec_columns)
    df.drop_duplicates(subset='Index', inplace=True)

    if not df['Index'].is_monotonic_increasing:
        df.sort_values('Index', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # Join temperature data
    aux_df = pd.DataFrame(aux, columns=aux_columns)
    aux_df.drop_duplicates(inplace=True)
    if not aux_df.empty:
        pvt_df = aux_df.pivot(index='Index', columns='Aux', values='T')
        for k in pvt_df.keys():
            pvt_df.rename(columns={k: f"T{k}"}, inplace=True)
        df = df.join(pvt_df, on='Index')

    # Postprocessing
    df['Step'] = _count_changes(df['Step'])
    if software_cycle_number:
        df['Cycle'] = _generate_cycle_number(df)
    df = df.astype(dtype=dtype_dict)

    return df


def _valid_record(bytes):
    """Helper function to identify a valid record"""
    # Check for a non-zero Status
    [Status] = struct.unpack('<B', bytes[12:13])
    return (Status != 0)


def _bytes_to_list(bytes):
    """Helper function for interpreting a byte string"""

    # Extract fields from byte string
    [Index, Cycle] = struct.unpack('<II', bytes[2:10])
    [Step] = struct.unpack('<I', bytes[10:14])
    [Status, Jump, Time] = struct.unpack('<BBQ', bytes[12:22])
    [Voltage, Current] = struct.unpack('<ii', bytes[22:30])
    [Charge_capacity, Discharge_capacity] = struct.unpack('<qq', bytes[38:54])
    [Charge_energy, Discharge_energy] = struct.unpack('<qq', bytes[54:70])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', bytes[70:77])
    [Range] = struct.unpack('<i', bytes[78:82])

    # Index and should not be zero
    if Index == 0 or Status == 0:
        return []

    multiplier = multiplier_dict[Range]

    # Create a dictionary for the record
    list = [
        Index,
        Cycle + 1,
        Step,
        state_dict[Status],
        Time/1000,
        Voltage/10000,
        Current*multiplier,
        Charge_capacity*multiplier/3600,
        Discharge_capacity*multiplier/3600,
        Charge_energy*multiplier/3600,
        Discharge_energy*multiplier/3600,
        datetime(Y, M, D, h, m, s)
    ]
    return list


def _aux_bytes_to_list(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux, Index] = struct.unpack('<BI', bytes[1:6])
    [T] = struct.unpack('<h', bytes[34:36])

    return [Index, Aux, T/10]


def _generate_cycle_number(df):
    """
    Generate a cycle number to match Neware. A new cycle starts with a charge
    step after there has previously been a discharge.
    """

    # Identify the beginning of charge steps
    chg = (df['Status'] == 'CCCV_Chg') | (df['Status'] == 'CC_Chg') |  (df['Status'] == 'CP_Chg')
    chg = (chg - chg.shift()).clip(0)
    chg.iat[0] = 1

    # Convert to numpy arrays
    chg = chg.values
    status = df['Status'].values

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

    return chg


def _count_changes(series):
    """Enumerate the number of value changes in a series"""
    a = series.diff()
    a.iloc[0] = 1
    a.iloc[-1] = 0
    return (abs(a) > 0).cumsum()
