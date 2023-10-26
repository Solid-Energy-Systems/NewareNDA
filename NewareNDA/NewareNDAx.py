# © 2023 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import mmap
import struct
import logging
import tempfile
import zipfile
import re
from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd

from NewareNDA.dicts import rec_columns, dtype_dict, state_dict, \
     multiplier_dict


def read_ndax(file):
    """
    Function to read electrochemical data from a Neware ndax binary file.

    Args:
        file (str): Name of an .ndax file to read
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        zf = zipfile.PyZipFile(file)

        # Read version information
        version_info = zf.extract('VersionInfo.xml', path=tmpdir)
        with open(version_info, 'r', encoding='gb2312') as f:
            root = ET.fromstring(f.read())
        server = root.find('config/ZwjVersion').attrib['SvrVer']
        client = root.find('config/ZwjVersion').attrib['CurrClientVer']
        logging.info(server)
        logging.info(client)

        # Check for unsupported versions
        if int(server[14]) > 7:
            raise NotImplementedError(f"{server} is not yet supported!")

        data_file = zf.extract('data.ndc', path=tmpdir)
        data_df, _ = read_ndc(data_file)

        # Read and merge Aux data from ndc files
        aux_df = pd.DataFrame([])
        for f in zf.namelist():
            m = re.search(".*_([0-9]+)[.]ndc", f)
            if m:
                aux_file = zf.extract(f, path=tmpdir)
                _, aux = read_ndc(aux_file)
                aux_df = pd.concat([aux_df, aux], ignore_index=True)
        if not aux_df.empty:
            pvt_df = aux_df.pivot(index='Index', columns='Aux')
            pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
            data_df = data_df.join(pvt_df, on='Index')

    return data_df


def read_ndc(file):
    """
    Function to read electrochemical data from a Neware ndc binary file.

    Args:
        file (str): Name of an .ndc file to read
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
        aux_df (pd.DataFrame): DataFrame containing any temperature data
    """
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        # Identify the beginning of the data section
        record_len = 94
        header = 517
        identifier = mm[517:525]

        # Read data records
        output = []
        aux = []
        while header != -1:
            mm.seek(header)
            bytes = mm.read(record_len)
            if bytes[0:1] == b'\x55':
                output.append(_bytes_to_list_ndc(bytes))
            elif bytes[0:1] == b'\x65':
                aux.append(_aux_bytes_65_to_list_ndc(bytes))
            elif bytes[0:1] == b'\x74':
                aux.append(_aux_bytes_74_to_list_ndc(bytes))
            else:
                logging.warning("Unknown record type: "+bytes[0:1].hex())
            header = mm.find(identifier, header + record_len)

    # Create DataFrame and sort by Index
    df = pd.DataFrame(output, columns=rec_columns)
    df.drop_duplicates(subset='Index', inplace=True)

    if not df['Index'].is_monotonic_increasing:
        df.sort_values('Index', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # Postprocessing
    aux_df = pd.DataFrame([])
    df = df.astype(dtype=dtype_dict)
    if identifier[0:1] == b'\x65':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T'])
    elif identifier[0:1] == b'\x74':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T', 't'])
    return df, aux_df


def _bytes_to_list_ndc(bytes):
    """Helper function for interpreting an ndc byte string"""

    # Extract fields from byte string
    [Index, Cycle] = struct.unpack('<II', bytes[8:16])
    [Step] = struct.unpack('<B', bytes[16:17])
    [Status] = struct.unpack('<B', bytes[17:18])
    [Time] = struct.unpack('<Q', bytes[23:31])
    [Voltage, Current] = struct.unpack('<ii', bytes[31:39])
    [Charge_capacity, Discharge_capacity] = struct.unpack('<qq', bytes[43:59])
    [Charge_energy, Discharge_energy] = struct.unpack('<qq', bytes[59:75])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', bytes[75:82])
    [Range] = struct.unpack('<i', bytes[82:86])

    multiplier = multiplier_dict[Range]

    # Create a record
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


def _aux_bytes_65_to_list_ndc(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack('<B', bytes[3:4])
    [Index] = struct.unpack('<I', bytes[8:12])
    [T] = struct.unpack('<h', bytes[41:43])
    [V] = struct.unpack('<i', bytes[31:35])

    return [Index, Aux, V/10000, T/10]


def _aux_bytes_74_to_list_ndc(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack('<B', bytes[3:4])
    [Index] = struct.unpack('<I', bytes[8:12])
    [V] = struct.unpack('<i', bytes[31:35])
    [T, t] = struct.unpack('<hh', bytes[41:45])

    return [Index, Aux, V/10000, T/10, t/10]
