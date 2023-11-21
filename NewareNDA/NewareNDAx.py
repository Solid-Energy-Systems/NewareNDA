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

import NewareNDA.NewareNDA
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
        try:
            with open(version_info, 'r', encoding='gb2312') as f:
                root = ET.fromstring(f.read())
            server = root.find('config/ZwjVersion').attrib['SvrVer']
            client = root.find('config/ZwjVersion').attrib['CurrClientVer']
            logging.info(server)
            logging.info(client)
        except Exception:
            pass

        data_file = zf.extract('data.ndc', path=tmpdir)

        # Some ndax have data spread across 3 different ndc files. Others have
        # all data in data.ndc.
        # Check if data_runInfo.ndc and data_step.ndc exist
        if all(i in zf.namelist() for i in ['data_runInfo.ndc', 'data_step.ndc']):

            # Read data from separate files
            runInfo_file = zf.extract('data_runInfo.ndc', path=tmpdir)
            step_file = zf.extract('data_step.ndc', path=tmpdir)
            data_df = _read_data_ndc(data_file)
            runInfo_df = _read_data_runInfo_ndc(runInfo_file)
            step_df = _read_data_step_ndc(step_file)

            # Merge dataframes
            data_df = data_df[data_df['Index'] <= runInfo_df['Index'].iat[-1]]
            data_df = data_df.merge(runInfo_df, how='left', on='Index')

            # Fill in missing data - Neware appears to fabricate data
            data_df['Step'] = data_df['Step'].ffill().astype(int)
            data_df.interpolate(method='linear', inplace=True)
            data_df['Timestamp'] = data_df['Timestamp'].astype(int).map(
                datetime.fromtimestamp)

            data_df = data_df.merge(step_df, how='left', on='Step').reindex(
                columns=rec_columns)

        else:
            data_df, _ = read_ndc(data_file)

            # Read and merge Aux data from ndc files
            aux_df = pd.DataFrame([])
            for f in zf.namelist():
                m = re.search(".*_([0-9]+)[.]ndc", f)
                if m:
                    aux_file = zf.extract(f, path=tmpdir)
                    _, aux = read_ndc(aux_file)
                    aux['Aux'] = int(m[1])
                    aux_df = pd.concat([aux_df, aux], ignore_index=True)
            if not aux_df.empty:
                pvt_df = aux_df.pivot(index='Index', columns='Aux')
                pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
                data_df = data_df.join(pvt_df, on='Index')

    return data_df


def _read_data_ndc(file):
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Identify the beginning of the data section
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<ff', bytes[132:-4]):
                rec.append([i[0]/10000, i[1]])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
    df['Index'] = df.index + 1
    return df


def _read_data_runInfo_ndc(file):
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Identify the beginning of the data section
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<isffff12siii2s', bytes[132:-63]):
                Time = i[0]
                [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
                [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
                [Timestamp, Step, Index] = [i[7], i[8], i[9]]
                if Index != 0:
                    rec.append([Time/1000,
                                Charge_Capacity/3600, Discharge_Capacity/3600,
                                Charge_Energy/3600, Discharge_Energy/3600,
                                Timestamp, Step, Index])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=[
        'Time',
        'Charge_Capacity(mAh)', 'Discharge_Capacity(mAh)',
        'Charge_Energy(mWh)', 'Discharge_Energy(mWh)',
        'Timestamp', 'Step', 'Index'])
    df['Step'] = NewareNDA.NewareNDA._count_changes(df['Step'])

    return df


def _read_data_step_ndc(file):
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Identify the beginning of the data section
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<ii16sb12s', bytes[132:-5]):
                [Cycle, Step_Index, Status] = [i[0], i[1], i[3]]
                if Step_Index != 0:
                    rec.append([Cycle+1, Step_Index, state_dict[Status]])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=['Cycle', 'Step_Index', 'Status'])
    df['Step'] = df.index + 1
    return df


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
        identifier = mm[517:525]
        id_byte = slice(0, 1)
        rec_byte = slice(0, 1)
        if identifier == b'\x00\x00\x00\x00\x00\x00\x00\x00':
            record_len = 90
            offset = 4
            identifier = mm[4225:4229]
            id_byte = slice(3, 4)
            rec_byte = slice(7, 8)

        # Read data records
        output = []
        aux = []
        header = mm.find(identifier)
        while header != -1:
            mm.seek(header - offset)
            bytes = mm.read(record_len)
            if _valid_record(bytes):
                if bytes[rec_byte] == b'\x55':
                    output.append(_bytes_to_list_ndc(bytes))
                elif bytes[rec_byte] == b'\x65':
                    aux.append(_aux_bytes_65_to_list_ndc(bytes))
                elif bytes[rec_byte] == b'\x74':
                    aux.append(_aux_bytes_74_to_list_ndc(bytes))
                else:
                    logging.warning("Unknown record type: "+bytes[rec_byte].hex())

            header = mm.find(identifier, header - offset + record_len)

    # Create DataFrame and sort by Index
    df = pd.DataFrame(output, columns=rec_columns)
    df.drop_duplicates(subset='Index', inplace=True)

    if not df['Index'].is_monotonic_increasing:
        df.sort_values('Index', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # Postprocessing
    aux_df = pd.DataFrame([])
    df = df.astype(dtype=dtype_dict)
    if identifier[id_byte] == b'\x65':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T'])
    elif identifier[id_byte] == b'\x74':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T', 't'])
    return df, aux_df


def _valid_record(bytes):
    """Helper function to identify a valid record"""
    [Status] = struct.unpack('<B', bytes[17:18])
    return (Status != 0) & (Status != 255)


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
