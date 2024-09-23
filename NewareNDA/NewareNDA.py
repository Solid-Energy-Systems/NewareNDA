# © 2022-2024 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import os
import mmap
import struct
import logging
from datetime import datetime, timezone
import pandas as pd

from .utils import _generate_cycle_number, _count_changes
from .dicts import rec_columns, dtype_dict, aux_dtype_dict, state_dict, \
    multiplier_dict
from .NewareNDAx import read_ndax

logger = logging.getLogger('newarenda')


def read(file, software_cycle_number=True, cycle_mode='chg', log_level='INFO'):
    """
    Read electrochemical data from an Neware nda or ndax binary file.

    Args:
        file (str): Name of an .nda or .ndax file to read
        software_cycle_number (bool): Regenerate the cycle number to match
            Neware's "Charge First" circular statistic setting
        cycle_mode (str): Selects how the cycle is incremented.
            'chg': (Default) Sets new cycles with a Charge step following a Discharge.
            'dchg': Sets new cycles with a Discharge step following a Charge.
            'auto': Identifies the first non-rest state as the incremental state.
        log_level (str): Sets the modules logging level. Default: 'INFO'
            Options: 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """

    # Set up logging
    log_level = log_level.upper()
    if log_level in logging._nameToLevel.keys():
        logger.setLevel(log_level)
    else:
        logger.warning(f"Logging level '{log_level}' not supported; Defaulting to 'INFO'. "
                       f"Supported options are: {', '.join(logging._nameToLevel.keys())}")

    # Identify file type and process accordingly
    _, ext = os.path.splitext(file)
    if ext == '.nda':
        return read_nda(file, software_cycle_number, cycle_mode)
    elif ext == '.ndax':
        return read_ndax(file, software_cycle_number, cycle_mode)
    else:
        logger.error("File type not supported!")
        raise TypeError("File type not supported!")


def read_nda(file, software_cycle_number, cycle_mode='chg'):
    """
    Function read electrochemical data from a Neware nda binary file.

    Args:
        file (str): Name of a .nda file to read
        software_cycle_number (bool): Generate the cycle number field
            to match old versions of BTSDA
        cycle_mode (str): Selects how the cycle is incremented.
            'chg': (Default) Sets new cycles with a Charge step following a Discharge.
            'dchg': Sets new cycles with a Discharge step following a Charge.
            'auto': Identifies the first non-rest state as the incremental state.
    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        if mm.read(6) != b'NEWARE':
            logger.error(f"{file} does not appear to be a Neware file.")
            raise ValueError(f"{file} does not appear to be a Neware file.")

        # Get the file version
        [nda_version] = struct.unpack('<B', mm[14:15])
        logger.info(f"NDA version: {nda_version}")

        # Try to find server and client version info
        version_loc = mm.find(b'BTSServer')
        if version_loc != -1:
            mm.seek(version_loc)
            server = mm.read(50).strip(b'\x00').decode()
            logger.info(f"Server: {server}")
            mm.seek(50, 1)
            client = mm.read(50).strip(b'\x00').decode()
            logger.info(f"Client: {client}")
        else:
            logger.info("BTS version not found!")

        # version specific settings
        if nda_version == 29:
            output, aux = _read_nda_29(mm)
        elif nda_version == 130:
            output, aux = _read_nda_130(mm)
        else:
            logger.error(f"nda version {nda_version} is not yet supported!")
            raise NotImplementedError(f"nda version {nda_version} is not yet supported!")

    # Create DataFrame and sort by Index
    df = pd.DataFrame(output, columns=rec_columns)
    df.drop_duplicates(subset='Index', inplace=True)

    if not df['Index'].is_monotonic_increasing:
        df.sort_values('Index', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # Join temperature data
    aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'T', 'V'])
    aux_df.drop_duplicates(inplace=True)
    if not aux_df.empty:
        aux_df = aux_df.astype(
            {k: aux_dtype_dict[k] for k in aux_dtype_dict.keys() & aux_df.columns})
        pvt_df = aux_df.pivot(index='Index', columns='Aux')
        pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
        df = df.join(pvt_df, on='Index')

    # Postprocessing
    df['Step'] = _count_changes(df['Step'])
    if software_cycle_number:
        df['Cycle'] = _generate_cycle_number(df, cycle_mode)
    df = df.astype(dtype=dtype_dict)

    return df


def _read_nda_29(mm):
    """Helper function for nda version 29"""
    mm_size = mm.size()

    # Get the active mass
    [active_mass] = struct.unpack('<I', mm[152:156])
    logger.info(f"Active mass: {active_mass/1000} mg")

    try:
        remarks = mm[2317:2417].decode('ASCII')
        # Clean null characters
        remarks = remarks.replace(chr(0), '').strip()
        logger.info(f"Remarks: {remarks}")
    except UnicodeDecodeError:
        logger.warning("Converting remark bytes into ASCII failed")
        remarks = ""

    # Identify the beginning of the data section
    record_len = 86
    identifier = b'\x00\x00\x00\x00\x55\x00'
    header = mm.find(identifier)
    if header == -1:
        logger.error("File does not contain any valid records.")
        raise EOFError("File does not contain any valid records.")
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

    return output, aux


def _read_nda_130(mm):
    """Helper function for nda version 130"""
    mm_size = mm.size()

    # Identify the beginning of the data section
    record_len = 88
    identifier = mm[1024:1030]
    if mm[1024:1025] == b'\x55':  # BTS 9.1
        # Find next record and get length
        record_len = mm.find(mm[1024:1026], 1026) - 1024
    mm.seek(1024)

    # Read data records
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # Check for a data record
            if bytes[0:1] == b'\x55':
                output.append(_bytes_to_list_BTS91(bytes))
                if record_len == 56:
                    aux.append(_aux_bytes_to_list_BTS91(bytes))
            elif bytes[0:6] == identifier:
                output.append(_bytes_to_list_BTS9(bytes[4:]))

            # Check for an auxiliary record
            elif bytes[0:5] == b'\x00\x00\x00\x00\x65':
                aux.append(_aux_bytes_to_list(bytes[4:]))

            elif bytes[0:1] == b'\x81':
                break

    # Find footer data block
    footer = mm.rfind(b'\x06\x00\xf0\x1d\x81\x00\x03\x00\x61\x90\x71\x90\x02\x7f\xff\x00', 1024)
    if footer != -1:
        mm.seek(footer+16)
        bytes = mm.read(499)

        # Get the active mass
        [active_mass] = struct.unpack('<d', bytes[-8:])
        logger.info(f"Active mass: {active_mass} mg")

        # Get the remarks
        remarks = bytes[363:491].decode('ASCII')

        # Clean null characters
        remarks = remarks.replace(chr(0), '').strip()
        logger.info(f"Remarks: {remarks}")

    return output, aux


def _valid_record(bytes):
    """Helper function to identify a valid record"""
    # Check for a non-zero Status
    [Status] = struct.unpack('<B', bytes[12:13])
    return (Status != 0)


def _bytes_to_list(bytes):
    """Helper function for interpreting a byte string"""

    # Extract fields from byte string
    [Index, Cycle, Step] = struct.unpack('<III', bytes[2:14])
    [Status, Jump, Time, Voltage, Current] = struct.unpack('<BBQii', bytes[12:30])
    [Charge_capacity, Discharge_capacity,
     Charge_energy, Discharge_energy] = struct.unpack('<qqqq', bytes[38:70])
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


def _bytes_to_list_BTS9(bytes):
    """Helper function to interpret byte strings from BTS9"""
    [Step, Status] = struct.unpack('<BB', bytes[5:7])
    [Index] = struct.unpack('<I', bytes[12:16])
    [Time, Voltage, Current] = struct.unpack('<Qff', bytes[24:40])
    [Charge_Capacity, Charge_Energy,
     Discharge_Capacity, Discharge_Energy,
     Date] = struct.unpack('<ffffQ', bytes[48:72])

    # Create a dictionary for the record
    list = [
        Index,
        0,
        Step,
        state_dict[Status],
        Time/1e6,
        Voltage,
        Current,
        Charge_Capacity/3600,
        Discharge_Capacity/3600,
        Charge_Energy/3600,
        Discharge_Energy/3600,
        datetime.fromtimestamp(Date/1e6, timezone.utc).astimezone()
    ]
    return list


def _bytes_to_list_BTS91(bytes):
    """Helper function to interpret byte strings from BTS9.1"""
    [Step, Status] = struct.unpack('<BB', bytes[2:4])
    [Index, Time, Time_ns] = struct.unpack('<III', bytes[8:20])
    [Current, Voltage, Capacity, Energy] = struct.unpack('<ffff', bytes[20:36])
    [Date, Date_ns] = struct.unpack('<II', bytes[44:52])

    # Convert capacity and energy to charge and discharge fields
    Charge_Capacity = 0 if Capacity < 0 else Capacity
    Discharge_Capacity = 0 if Capacity > 0 else abs(Capacity)
    Charge_Energy = 0 if Energy < 0 else Energy
    Discharge_Energy = 0 if Energy > 0 else abs(Energy)

    # Create a dictionary for the record
    list = [
        Index,
        0,
        Step,
        state_dict[Status],
        Time + 1e-9*Time_ns,
        Voltage,
        Current,
        Charge_Capacity/3600,
        Discharge_Capacity/3600,
        Charge_Energy/3600,
        Discharge_Energy/3600,
        datetime.fromtimestamp(Date + 1e-9*Date_ns, timezone.utc).astimezone()
    ]
    return list


def _aux_bytes_to_list(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux, Index] = struct.unpack('<BI', bytes[1:6])
    [V] = struct.unpack('<i', bytes[22:26])
    [T] = struct.unpack('<h', bytes[34:36])

    return [Index, Aux, T/10, V/10000]


def _aux_bytes_to_list_BTS91(bytes):
    [Index] = struct.unpack('<I', bytes[8:12])
    [T] = struct.unpack('<f', bytes[52:56])
    return [Index, 1, T, None]
