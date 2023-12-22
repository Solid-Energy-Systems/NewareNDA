# © 2023 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import mmap
import struct
import warnings
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


def read_ndax(file, software_cycle_number=False, cycle_mode='chg'):
    """
    Function to read electrochemical data from a Neware ndax binary file.

    Args:
        file (str): Name of an .ndax file to read
        software_cycle_number (bool): Regenerate the cycle number field
        cycle_mode (str): Selects how the cycle is incremented.
            'chg': (Default) Sets new cycles with a Charge step following a Discharge.
            'dchg': Sets new cycles with a Discharge step following a Charge.
            'auto': Identifies the first non-rest state as the incremental state.
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        zf = zipfile.PyZipFile(file)

        # Read version information
        version_info = zf.extract('VersionInfo.xml', path=tmpdir)
        try:
            with open(version_info, 'r', encoding='gb2312') as f:
                config = ET.fromstring(f.read()).find('config/ZwjVersion')
            logging.info(f"SvrVer: {config.attrib['SvrVer']}")
            logging.info(f"CurrClientVer: {config.attrib['CurrClientVer']}")
            logging.info(f"ZwjVersion: {config.attrib['ZwjVersion']}")
            logging.info(f"MainXwjVer: {config.attrib['MainXwjVer']}")
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
            data_df = data_df.merge(runInfo_df, how='left', on='Index')
            data_df['Step'].ffill(inplace=True)
            data_df = data_df.merge(step_df, how='left', on='Step').reindex(
                columns=rec_columns)

            # Fill in missing data - Neware appears to fabricate data
            _data_interpolation(data_df)

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

    if software_cycle_number:
        data_df['Cycle'] = NewareNDA.NewareNDA._generate_cycle_number(data_df, cycle_mode)

    return data_df.astype(dtype=dtype_dict)


def _data_interpolation(df):
    """
    Some ndax from from BTS Server 8 do not seem to contain a complete dataset.
    This helper function fills in missing times, capacities, and energies.
    """
    # Identify the valid data
    nan_mask = df['Time'].notnull()

    if nan_mask.any():
        warnings.warn("IMPORTANT: This ndax has missing data. The output from "
                      "NewareNDA contains interpolated data!")

    # Group by step and run 'inside' interpolation on Time
    df['Time'] = df.groupby('Step')['Time'].transform(
        lambda x: pd.Series.interpolate(x, limit_area='inside'))

    # Perform extrapolation to generate the remaining missing Time
    nan_mask2 = df['Time'].notnull()
    time_inc = df['Time'].diff().ffill().groupby(nan_mask2.cumsum()).cumsum()
    time = df['Time'].ffill() + time_inc.shift()
    df['Time'].where(nan_mask2, time, inplace=True)

    # Fill in missing Timestamps
    time_inc = df['Time'].diff().groupby(nan_mask.cumsum()).cumsum()
    timestamp = df['Timestamp'].ffill() + \
        pd.to_timedelta(time_inc.shift(), unit='S')
    df['Timestamp'].where(nan_mask, timestamp, inplace=True)

    # Integrate to get capacity and fill missing values
    capacity = df['Time'].diff()*abs(df['Current(mA)'])/3600
    inc = capacity.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Capacity(mAh)'].where(nan_mask, chg, inplace=True)
    df['Discharge_Capacity(mAh)'].where(nan_mask, dch, inplace=True)

    # Integrate to get energy and fill missing values
    energy = capacity*df['Voltage']
    inc = energy.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Energy(mWh)'].where(nan_mask, chg, inplace=True)
    df['Discharge_Energy(mWh)'].where(nan_mask, dch, inplace=True)


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
                if (i[0] != 0):
                    rec.append([i[0]/10000, i[1]])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
    df['Index'] = df.index + 1
    return df


def _read_data_runInfo_ndc(file):
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Choose byte format based on ndc file version
        [ndc_version] = struct.unpack('<B', mm[2:3])
        format = '<isffff12siii2s'
        end_byte = -63
        if ndc_version >= 14:
            format = '<isffff12siii10s'
            end_byte = -59

        # Identify the beginning of the data section
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack(format, bytes[132:end_byte]):
                Time = i[0]
                [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
                [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
                [Timestamp, Step, Index] = [i[7], i[8], i[9]]
                if Index != 0:
                    rec.append([Time/1000,
                                Charge_Capacity/3600, Discharge_Capacity/3600,
                                Charge_Energy/3600, Discharge_Energy/3600,
                                datetime.fromtimestamp(Timestamp), Step, Index])

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

        # Get ndc file version
        [ndc_version] = struct.unpack('<B', mm[2:3])
        logging.info(f"NDC version: {ndc_version}")

        # Identify the beginning of the data section
        record_len = 94
        offset = 0
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
            if bytes[rec_byte] == b'\x55':
                if _valid_record(bytes):
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
    aux_df.drop_duplicates(subset='Index', inplace=True)
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
