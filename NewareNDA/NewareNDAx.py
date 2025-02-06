# © 2022-2024 Copyright SES AI
# Author: Daniel Cogswell
# Email: danielcogswell@ses.ai

import sys
import mmap
import struct
import logging
import tempfile
import zipfile
import re
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET
import pandas as pd

from .utils import _generate_cycle_number, _count_changes
from .dicts import rec_columns, dtype_dict, aux_dtype_dict, state_dict, \
    multiplier_dict

logger = logging.getLogger('newarenda')


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

    ndax_file = NDAx(file)
    ndax_file.read_ndax(software_cycle_number, cycle_mode)
    return ndax_file.data_df

def _data_interpolation(df):
    """
    Some ndax from from BTS Server 8 do not seem to contain a complete dataset.
    This helper function fills in missing times, capacities, and energies.
    """
    logger.warning("IMPORTANT: This ndax has missing data. The output from "
                   "NewareNDA contains interpolated data!")

    # Identify the valid data
    nan_mask = df['Time'].notnull()

    # Group by step and run 'inside' interpolation on Time
    df['Time'] = df.groupby('Step')['Time'].transform(
        lambda x: pd.Series.interpolate(x, limit_area='inside'))

    # Perform extrapolation to generate the remaining missing Time
    nan_mask2 = df['Time'].notnull()
    time_inc = df['Time'].diff().ffill().groupby(nan_mask2.cumsum()).cumsum()
    time = df['Time'].ffill() + time_inc.shift()
    df['Time'] = df['Time'].where(nan_mask2, time)

    # Fill in missing Timestamps
    time_inc = df['Time'].diff().groupby(nan_mask.shift().cumsum()).cumsum()
    timestamp = df['Timestamp'].ffill() + \
        pd.to_timedelta(time_inc.fillna(0), unit='s')
    df['Timestamp'] = df['Timestamp'].where(nan_mask, timestamp)

    # Integrate to get capacity and fill missing values
    capacity = df['Time'].diff()*abs(df['Current(mA)'])/3600
    inc = capacity.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Capacity(mAh)'] = df['Charge_Capacity(mAh)'].where(nan_mask, chg)
    df['Discharge_Capacity(mAh)'] = df['Discharge_Capacity(mAh)'].where(nan_mask, dch)

    # Integrate to get energy and fill missing values
    energy = capacity*df['Voltage']
    inc = energy.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Energy(mWh)'] = df['Charge_Energy(mWh)'].where(nan_mask, chg)
    df['Discharge_Energy(mWh)'] = df['Discharge_Energy(mWh)'].where(nan_mask, dch)


def _bytes_to_list_ndc(bytes):
    """Helper function for interpreting an ndc byte string"""

    # Extract fields from byte string
    [Index, Cycle, Step, Status] = struct.unpack('<IIBB', bytes[8:18])
    [Time, Voltage, Current] = struct.unpack('<Qii', bytes[23:39])
    [Charge_capacity, Discharge_capacity,
     Charge_energy, Discharge_energy] = struct.unpack('<qqqq', bytes[43:75])
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


class NDAx:
    """
    Object to store all gathered data from an ndax file

    Args:
        filename (str): Name of an .ndax file to read
    """
    def __init__(self, filename):
        self.filename = filename

        self.barcode = None
        self.active_mass = None # mg
        self.start_time = None
        self.PN = None
        self.aux_ch_dict = {}
        self.filelist = []
        self.data_df = {}

        self.server_version = None
        self.client_version = None
        self.control_unit_version = None
        self.tester_version = None

    def read_ndax(self, software_cycle_number=False, cycle_mode='chg'):
        """
        Function to read electrochemical data from a Neware ndax
        binary file. Populates self.data_df with the read data.

        Args:
            software_cycle_number (bool): Regenerate the cycle number field
            cycle_mode (str): Selects how the cycle is incremented.
                'chg': (Default) Sets new cycles with a Charge step following a Discharge.
                'dchg': Sets new cycles with a Discharge step following a Charge.
                'auto': Identifies the first non-rest state as the incremental state.

        """
        with tempfile.TemporaryDirectory() as tmpdir:
            zf = zipfile.PyZipFile(self.filename)
            self.filelist = zf.namelist()

            # Read version information
            if 'VersionInfo.xml' in self.filelist:
                version_info = zf.extract('VersionInfo.xml', path=tmpdir)
                with open(version_info, 'r', encoding='gb2312') as f:
                    config = ET.fromstring(f.read()).find('config/ZwjVersion')

                if 'SvrVer' in config.attrib:
                    self.server_version = config.attrib['SvrVer']
                    logger.info(f"Server version: {self.server_version}")
                if 'CurrClientVer' in config.attrib:
                    self.client_version = config.attrib['CurrClientVer']
                    logger.info(f"Client version: {self.client_version}")
                if 'ZwjVersion' in config.attrib:
                    self.control_unit_version = config.attrib['ZwjVersion']
                    logger.info(f"Control unit version: {self.control_unit_version}")
                if 'MainXwjVer' in config.attrib:
                    self.tester_version = config.attrib['MainXwjVer']
                    logger.info(f"Tester version: {self.tester_version}")

            # Read active mass.
            # TODO: if test is edited while running then there are
            # Step{1,2,3,..}.xml files. We should perhaps check the newest
            # one rather than Step.xml in case active mass was changed.
            if 'Step.xml' in self.filelist:
                step = zf.extract('Step.xml', path=tmpdir)
                with open(step, 'r', encoding='gb2312') as f:
                    config = ET.fromstring(f.read()).find('config')
                if 'Head_Info/SCQ' in config.attrib:
                    active_mass = float(config.find('Head_Info/SCQ').attrib['Value'])
                    logger.info(f"Active mass: {active_mass/1000} mg")

            # Read aux channel mapping and test information from
            # TestInfo.xml
            # TODO: if test is edited while running then there are
            # TestInfo{1,2,3,..}.xml files. Aux mapping and start time are
            # the same but the Barcode and SN might have changed, so we
            # should perhaps read newest TestInfo.xml file.
            if 'TestInfo.xml' in self.filelist:
                step = zf.extract('TestInfo.xml', path=tmpdir)
                with open(step, 'r', encoding='gb2312') as f:
                    config = ET.fromstring(f.read()).find('config')

                if 'Barcode' in config.find("TestInfo").attrib:
                    logger.info(f"Test barcode: {config.find('TestInfo').attrib['Barcode']}")

                if 'SN' in config.find("TestInfo").attrib:
                    logger.info(f"Test P/N: {config.find('TestInfo').attrib['SN']}")

                self.start_time = datetime.strptime(config.find('TestInfo').attrib['StartTime'], '%Y-%m-%d %H:%M:%S')
                logger.info(f"Test start time: {self.start_time}")

                num_of_aux = int(config.find("TestInfo").attrib["AuxCount"])
                for num in range(1, num_of_aux+1):
                    aux = config.find(f"TestInfo/Aux{num}")
                    self.aux_ch_dict.update({int(aux.attrib['RealChlID']): int(aux.attrib['AuxID'])})

            # Try to read data.ndc
            if 'data.ndc' in self.filelist:
                data_file = zf.extract('data.ndc', path=tmpdir)
                self.data_df = self.read_ndc(data_file)
            else:
                raise NotImplementedError("File type not yet supported!")

            # Some ndax have data spread across 3 different ndc files. Others have
            # all data in data.ndc.
            # Check if data_runInfo.ndc and data_step.ndc exist
            if all(i in self.filelist for i in ['data_runInfo.ndc', 'data_step.ndc']):

                # Read data from separate files
                runInfo_file = zf.extract('data_runInfo.ndc', path=tmpdir)
                step_file = zf.extract('data_step.ndc', path=tmpdir)
                runInfo_df = self.read_ndc(runInfo_file)
                step_df = self.read_ndc(step_file)

                # Merge dataframes
                self.data_df = self.data_df.merge(runInfo_df, how='left', on='Index')
                self.data_df['Step'] = self.data_df['Step'].ffill()
                self.data_df = self.data_df.merge(step_df, how='left', on='Step').reindex(
                    columns=rec_columns)

                # Fill in missing data - Neware appears to fabricate data
                if self.data_df.isnull().any(axis=None):
                    _data_interpolation(self.data_df)

            # Read and merge Aux data from ndc files
            aux_df = pd.DataFrame([])
            for f in self.filelist:

                # If the filename contains a channel number, convert to aux_id
                m = re.search("data_AUX_([0-9]+)_[0-9]+_[0-9]+[.]ndc", f)
                if m:
                    ch = int(m[1])
                    aux_id = self.aux_ch_dict[ch]
                else:
                    m = re.search(".*_([0-9]+)[.]ndc", f)
                    if m:
                        aux_id = int(m[1])

                if m:
                    aux_file = zf.extract(f, path=tmpdir)
                    aux = self.read_ndc(aux_file)
                    aux['Aux'] = aux_id
                    aux_df = pd.concat([aux_df, aux], ignore_index=True)
            if not aux_df.empty:
                aux_df = aux_df.astype(
                    {k: aux_dtype_dict[k] for k in aux_dtype_dict.keys() & aux_df.columns})
                pvt_df = aux_df.pivot(index='Index', columns='Aux')
                pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
                self.data_df = self.data_df.join(pvt_df, on='Index')

        if software_cycle_number:
            self.data_df['Cycle'] = _generate_cycle_number(self.data_df, cycle_mode)

        self.data_df = self.data_df.astype(dtype=dtype_dict)

    def read_ndc(self, file):
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

            # Get ndc file version and filetype
            [ndc_filetype] = struct.unpack('<B', mm[0:1])
            [ndc_version] = struct.unpack('<B', mm[2:3])
            logger.debug(f"NDC version: {ndc_version} filetype: {ndc_filetype}")

            try:
                f = getattr(sys.modules[__name__].NDAx, f"_read_ndc_{ndc_version}_filetype_{ndc_filetype}")
                return f(self, mm)
            except AttributeError:
                raise NotImplementedError(f"ndc version {ndc_version} filetype {ndc_filetype} is not yet supported!")


    def _read_ndc_2_filetype_1(self, mm):
        record_len = 94
        identifier = mm[517:525]

        # Read data records
        output = []
        header = mm.find(identifier)
        while header != -1:
            mm.seek(header)
            bytes = mm.read(record_len)
            if bytes[0:1] == b'\x55':
                output.append(_bytes_to_list_ndc(bytes))
            else:
                logger.warning("Unknown record type: "+bytes[0:1].hex())

            header = mm.find(identifier, header + record_len)

        # Postprocessing
        df = pd.DataFrame(output, columns=rec_columns)

        return df


    def _read_ndc_2_filetype_5(self, mm):
        record_len = 94
        identifier = mm[517:525]

        # Read aux records
        aux = []
        header = mm.find(identifier)
        while header != -1:
            mm.seek(header)
            bytes = mm.read(record_len)
            if bytes[0:1] == b'\x65':
                aux.append(_aux_bytes_65_to_list_ndc(bytes))
            elif bytes[0:1] == b'\x74':
                aux.append(_aux_bytes_74_to_list_ndc(bytes))
            else:
                logger.warning("Unknown record type: "+bytes[0:1].hex())

            header = mm.find(identifier, header + record_len)

        # Postprocessing
        aux_df = pd.DataFrame([])
        if identifier[0:1] == b'\x65':
            aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T'])
        elif identifier[0:1] == b'\x74':
            aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T', 't'])

        return aux_df


    def _read_ndc_5_filetype_1(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        output = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<87s', bytes[125:-56]):
                if i[0][7:8] == b'\x55':
                    output.append(_bytes_to_list_ndc(i[0]))

        # Postprocessing
        df = pd.DataFrame(output, columns=rec_columns)

        return df


    def _read_ndc_5_filetype_5(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read aux records
        aux65 = []
        aux74 = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<87s', bytes[125:-56]):
                if i[0][7:8] == b'\x65':
                    aux65.append(_aux_bytes_65_to_list_ndc(i[0]))
                elif i[0][7:8] == b'\x74':
                    aux74.append(_aux_bytes_74_to_list_ndc(i[0]))

        # Concat aux65 and aux74 if they both contain data
        aux_df = pd.DataFrame(aux65, columns=['Index', 'Aux', 'V', 'T'])
        aux74_df = pd.DataFrame(aux74, columns=['Index', 'Aux', 'V', 'T', 't'])
        if (not aux_df.empty) & (not aux74_df.empty):
            aux_df = pd.concat([aux_df, aux74_df.drop(columns=['t'])])
        elif (not aux74_df.empty):
            aux_df = aux74_df

        return aux_df


    def _read_ndc_11_filetype_1(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<ff', bytes[132:-4]):
                if (i[0] != 0):
                    rec.append([1e-4*i[0], i[1]])

        # Create DataFrame
        df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
        df['Index'] = df.index + 1
        return df


    def _read_ndc_11_filetype_5(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        aux = []
        mm.seek(header)

        if mm[header+132:header+133] == b'\x65':
            while mm.tell() < mm_size:
                bytes = mm.read(record_len)
                for i in struct.iter_unpack('<cfh', bytes[132:-2]):
                    if i[0] == b'\x65':
                        aux.append([i[1]/10000, i[2]/10])

            # Create DataFrame
            aux_df = pd.DataFrame(aux, columns=['V', 'T'])
            aux_df['Index'] = aux_df.index + 1

        elif mm[header+132:header+133] == b'\x74':
            while mm.tell() < mm_size:
                bytes = mm.read(record_len)
                for i in struct.iter_unpack('<cib29sh51s', bytes[132:-4]):
                    if i[0] == b'\x74':
                        aux.append([i[1], i[2], i[4]/10])

            # Create DataFrame
            aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'T'])

        return aux_df


    def _read_ndc_11_filetype_7(self, mm):
        mm_size = mm.size()
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


    def _read_ndc_11_filetype_18(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        first = True
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<isffff12siii2s', bytes[132:-63]):
                Time = i[0]
                [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
                [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
                Timestamp = datetime.fromtimestamp(i[7], timezone.utc)
                [Step, Index] = [i[8], i[9]]

                if first:
                    # Figure out timezone by comparing with StartTime in
                    # TestInfo.xml. The first timestamp seem to be 1 s or
                    # less after StartTime. Check for > 2 s to give it
                    # some wiggle room.
                    seconds = self.start_time.replace(tzinfo=timezone.utc) - Timestamp
                    if abs(seconds) > timedelta(seconds = 2):
                        # Get time difference rounded to nearest 30 min
                        unit_seconds = timedelta(minutes = 30).total_seconds()
                        half_over = seconds.total_seconds() + unit_seconds / 2
                        rounded_seconds = half_over - (half_over % unit_seconds)
                        time_delta = timedelta(seconds = rounded_seconds)
                    else:
                        time_delta = timedelta(seconds = 0)
                first = False

                # Return timestamp in absolute time of test machine, to
                # get consistent behaviour with older ndc versions.
                Timestamp = Timestamp + time_delta

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
            'Timestamp', 'Step', 'Index']).astype({'Time': 'float'})
        df['Step'] = _count_changes(df['Step'])
        df['Timestamp'] = df['Timestamp'].dt.tz_convert(None)

        return df


    def _read_ndc_14_filetype_1(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<ff', bytes[132:-4]):
                if (i[0] != 0):
                    rec.append([i[0], 1000*i[1]])

        # Create DataFrame
        df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
        df['Index'] = df.index + 1
        return df


    def _read_ndc_14_filetype_5(self, mm):
        record_len = 4096
        header = 4096

        # Read data records
        aux = []
        mm.seek(header)
        while mm.tell() < mm.size():
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<f', bytes[132:-4]):
                aux.append(i[0])

        # Create DataFrame
        aux_df = pd.DataFrame(aux, columns=['T'])
        aux_df['Index'] = aux_df.index + 1

        return aux_df


    def _read_ndc_14_filetype_7(self, mm):
        return self._read_ndc_11_filetype_7(mm)


    def _read_ndc_14_filetype_18(self, mm):
        mm_size = mm.size()
        record_len = 4096
        header = 4096

        # Read data records
        rec = []
        mm.seek(header)
        first = True
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<isffff12siii10s', bytes[132:-59]):
                Time = i[0]
                [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
                [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
                Timestamp = datetime.fromtimestamp(i[7], timezone.utc)
                [Step, Index] = [i[8], i[9]]

                if Index != 0:
                    if first:
                        # Figure out timezone by comparing with StartTime in
                        # TestInfo.xml. The first timestamp seem to be 1 s or
                        # less after StartTime. Check for > 2 s to give it
                        # some wiggle room.
                        seconds = self.start_time.replace(tzinfo=timezone.utc) - Timestamp
                        if abs(seconds) > timedelta(seconds = 2):
                            # Get time difference rounded to nearest 30 min
                            unit_seconds = timedelta(minutes = 30).total_seconds()
                            half_over = seconds.total_seconds() + unit_seconds / 2
                            rounded_seconds = half_over - (half_over % unit_seconds)
                            time_delta = timedelta(seconds = rounded_seconds)
                        else:
                            time_delta = timedelta(seconds = 0)

                    first = False

                    # Return timestamp in absolute time of test machine, to
                    # get consistent behaviour with older ndc versions.
                    Timestamp = Timestamp + time_delta

                    rec.append([Time/1000,
                                Charge_Capacity*1000, Discharge_Capacity*1000,
                                Charge_Energy*1000, Discharge_Energy*1000,
                                Timestamp, Step, Index])

        # Create DataFrame
        df = pd.DataFrame(rec, columns=[
            'Time',
            'Charge_Capacity(mAh)', 'Discharge_Capacity(mAh)',
            'Charge_Energy(mWh)', 'Discharge_Energy(mWh)',
            'Timestamp', 'Step', 'Index']).astype({'Time': 'float'})
        df['Step'] = _count_changes(df['Step'])
        df['Timestamp'] = df['Timestamp'].dt.tz_convert(None)

        return df
