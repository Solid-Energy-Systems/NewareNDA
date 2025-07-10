# © 2022-2024 版权所有 SES AI
# 作者：Daniel Cogswell
# 邮箱：danielcogswell@ses.ai

import sys
import mmap
import struct
import logging
import tempfile
import zipfile
import re
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import pandas as pd

from .utils import _generate_cycle_number, _count_changes
from .dicts import rec_columns, dtype_dict, aux_dtype_dict, state_dict, \
    multiplier_dict

logger = logging.getLogger('newarenda')


def read_ndax(file, software_cycle_number=False, cycle_mode='chg'):
    """
    从 Neware ndax 二进制文件中读取电化学数据的函数。

    参数：
        file (str)：要读取的 .ndax 文件名
        software_cycle_number (bool)：重新生成循环编号字段
        cycle_mode (str)：选择循环递增方式。
            'chg': (默认) 在放电后以充电步骤设置新循环。
            'dchg': 在充电后以放电步骤设置新循环。
            'auto': 将第一个非静置状态识别为递增状态。
    返回：
        df (pd.DataFrame)：包含文件中所有记录的 DataFrame
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        zf = zipfile.PyZipFile(file)

        # 读取版本信息
        try:
            version_info = zf.extract('VersionInfo.xml', path=tmpdir)
            with open(version_info, 'r', encoding='gb2312') as f:
                config = ET.fromstring(f.read()).find('config/ZwjVersion')
            logger.info(f"Server version: {config.attrib['SvrVer']}")
            logger.info(f"Client version: {config.attrib['CurrClientVer']}")
            logger.info(f"Control unit version: {config.attrib['ZwjVersion']}")
            logger.info(f"Tester version: {config.attrib['MainXwjVer']}")
        except Exception:
            pass

        # 读取活性物质质量
        try:
            step = zf.extract('Step.xml', path=tmpdir)
            with open(step, 'r', encoding='gb2312') as f:
                config = ET.fromstring(f.read()).find('config')
            active_mass = float(config.find('Head_Info/SCQ').attrib['Value'])
            logger.info(f"Active mass: {active_mass/1000} mg")
        except Exception:
            pass

        # 从 TestInfo.xml 读取辅助通道映射
        aux_ch_dict = {}
        try:
            step = zf.extract('TestInfo.xml', path=tmpdir)
            with open(step, 'r', encoding='gb2312') as f:
                config = ET.fromstring(f.read()).find('config')

            for child in config.find("TestInfo"):
                aux_ch_dict.update({int(child.attrib['RealChlID']): int(child.attrib['AuxID'])})

        except Exception:
            pass

        # 尝试读取 data.ndc
        if 'data.ndc' in zf.namelist():
            data_file = zf.extract('data.ndc', path=tmpdir)
            data_df = read_ndc(data_file)
        else:
            raise NotImplementedError("File type not yet supported!")

        # 有些 ndax 文件的数据分散在 3 个不同的 ndc 文件中。其他文件则将所有数据放在 data.ndc 中。
        # 检查 data_runInfo.ndc 和 data_step.ndc 是否存在
        if all(i in zf.namelist() for i in ['data_runInfo.ndc', 'data_step.ndc']):

            # 合并数据帧
            runInfo_file = zf.extract('data_runInfo.ndc', path=tmpdir)
            step_file = zf.extract('data_step.ndc', path=tmpdir)
            runInfo_df = read_ndc(runInfo_file)
            step_df = read_ndc(step_file)

            # Merge dataframes
            data_df = data_df.merge(runInfo_df, how='left', on='Index')
            data_df['Step'] = data_df['Step'].ffill()
            data_df = data_df.merge(step_df, how='left', on='Step').reindex(
                columns=rec_columns)

            # 填充缺失数据 - Neware 似乎会伪造数据
            if data_df.isnull().any(axis=None):
                _data_interpolation(data_df)

        # 从 ndc 文件中读取并合并辅助数据
        aux_df = pd.DataFrame([])
        for f in zf.namelist():

            # 如果文件名包含通道号，则转换为 aux_id
            m = re.search("data_AUX_([0-9]+)_[0-9]+_[0-9]+[.]ndc", f)
            if m:
                ch = int(m[1])
                aux_id = aux_ch_dict[ch]
            else:
                m = re.search(".*_([0-9]+)[.]ndc", f)
                if m:
                    aux_id = int(m[1])

            if m:
                aux_file = zf.extract(f, path=tmpdir)
                aux = read_ndc(aux_file)
                aux['Aux'] = aux_id
                aux_df = pd.concat([aux_df, aux], ignore_index=True)
        if not aux_df.empty:
            aux_df = aux_df.astype(
                {k: aux_dtype_dict[k] for k in aux_dtype_dict.keys() & aux_df.columns})
            pvt_df = aux_df.pivot(index='Index', columns='Aux')
            pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
            data_df = data_df.join(pvt_df, on='Index')

    if software_cycle_number:
        data_df['Cycle'] = _generate_cycle_number(data_df, cycle_mode)

    return data_df.astype(dtype=dtype_dict)


def _data_interpolation(df):
    """
    来自 BTS Server 8 的某些 ndax 文件似乎不包含完整数据集。
    此辅助函数填充缺失的时间、容量和能量。
    """
    logger.warning("IMPORTANT: This ndax has missing data. The output from "
                   "NewareNDA contains interpolated data!")

    # 识别有效数据
    nan_mask = df['Time'].notnull()

    # 按步骤分组并对时间执行"内部"插值
    df['Time'] = df.groupby('Step')['Time'].transform(
        lambda x: pd.Series.interpolate(x, limit_area='inside'))

    # 执行外推以生成剩余的缺失时间
    nan_mask2 = df['Time'].notnull()
    time_inc = df['Time'].diff().ffill().groupby(nan_mask2.cumsum()).cumsum()
    time = df['Time'].ffill() + time_inc.shift()
    df['Time'] = df['Time'].where(nan_mask2, time)

    # 填充缺失的时间戳
    time_inc = df['Time'].diff().groupby(nan_mask.shift().cumsum()).cumsum()
    timestamp = df['Timestamp'].ffill() + \
        pd.to_timedelta(time_inc.fillna(0), unit='s')
    df['Timestamp'] = df['Timestamp'].where(nan_mask, timestamp)

    # 积分获取容量并填充缺失值
    capacity = df['Time'].diff()*abs(df['Current(mA)'])/3600
    inc = capacity.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Capacity(mAh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Capacity(mAh)'] = df['Charge_Capacity(mAh)'].where(nan_mask, chg)
    df['Discharge_Capacity(mAh)'] = df['Discharge_Capacity(mAh)'].where(nan_mask, dch)

    # 积分获取能量并填充缺失值
    energy = capacity*df['Voltage']
    inc = energy.groupby(nan_mask.cumsum()).cumsum()
    chg = df['Charge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] > 0, 0).shift()
    dch = df['Discharge_Energy(mWh)'].ffill() + \
        inc.where(df['Current(mA)'] < 0, 0).shift()
    df['Charge_Energy(mWh)'] = df['Charge_Energy(mWh)'].where(nan_mask, chg)
    df['Discharge_Energy(mWh)'] = df['Discharge_Energy(mWh)'].where(nan_mask, dch)


def read_ndc(file):
    """
    从 Neware ndc 二进制文件中读取电化学数据的函数。

    参数：
        file (str)：要读取的 .ndc 文件名
    返回：
        df (pd.DataFrame)：包含文件中所有记录的 DataFrame
        aux_df (pd.DataFrame)：包含任何温度数据的 DataFrame
    """
    with open(file, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        # 获取 ndc 文件版本和文件类型
        [ndc_filetype] = struct.unpack('<B', mm[0:1])
        [ndc_version] = struct.unpack('<B', mm[2:3])
        logger.debug(f"NDC version: {ndc_version} filetype: {ndc_filetype}")

        try:
            f = getattr(sys.modules[__name__], f"_read_ndc_{ndc_version}_filetype_{ndc_filetype}")
            return f(mm)
        except AttributeError:
            raise NotImplementedError(f"ndc version {ndc_version} filetype {ndc_filetype} is not yet supported!")


def _read_ndc_2_filetype_1(mm):
    record_len = 94
    identifier = mm[517:525]

    # 读取数据记录
    output = []
    header = mm.find(identifier)
    while header != -1:
        mm.seek(header)
        bytes = mm.read(record_len)
        if bytes[0:1] == b'\x55':
            output.append(_bytes_to_list_ndc(bytes))
        else:
            logger.warning("未知记录类型：" + bytes[0:1].hex())

        header = mm.find(identifier, header + record_len)

    # 后处理
    df = pd.DataFrame(output, columns=rec_columns)

    return df


def _read_ndc_2_filetype_5(mm):
    record_len = 94
    identifier = mm[517:525]

    # 读取辅助记录
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
            logger.warning("未知记录类型：" + bytes[0:1].hex())

        header = mm.find(identifier, header + record_len)

    # 后处理
    aux_df = pd.DataFrame([])
    if identifier[0:1] == b'\x65':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T'])
    elif identifier[0:1] == b'\x74':
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'V', 'T', 't'])

    return aux_df


def _read_ndc_5_filetype_1(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    output = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<87s', bytes[125:-56]):
            if i[0][7:8] == b'\x55':
                output.append(_bytes_to_list_ndc(i[0]))

    # 后处理
    df = pd.DataFrame(output, columns=rec_columns)

    return df


def _read_ndc_5_filetype_5(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取辅助记录
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

    # 如果 aux_df 和 aux74_df 都包含数据，则连接它们
    aux_df = pd.DataFrame(aux65, columns=['Index', 'Aux', 'V', 'T'])
    aux74_df = pd.DataFrame(aux74, columns=['Index', 'Aux', 'V', 'T', 't'])
    if (not aux_df.empty) & (not aux74_df.empty):
        aux_df = pd.concat([aux_df, aux74_df.drop(columns=['t'])])
    elif (not aux74_df.empty):
        aux_df = aux74_df

    return aux_df


def _read_ndc_11_filetype_1(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    rec = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<ff', bytes[132:-4]):
            if (i[0] != 0):
                rec.append([1e-4*i[0], i[1]])

    # 创建 DataFrame
    df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
    df['Index'] = df.index + 1
    return df


def _read_ndc_11_filetype_5(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    aux = []
    mm.seek(header)

    if mm[header+132:header+133] == b'\x65':
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<cfh', bytes[132:-2]):
                if i[0] == b'\x65':
                    aux.append([i[1]/10000, i[2]/10])

        # 创建 DataFrame
        aux_df = pd.DataFrame(aux, columns=['V', 'T'])
        aux_df['Index'] = aux_df.index + 1

    elif mm[header+132:header+133] == b'\x74':
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack('<cib29sh51s', bytes[132:-4]):
                if i[0] == b'\x74':
                    aux.append([i[1], i[2], i[4]/10])

        # 创建 DataFrame
        aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'T'])

    return aux_df


def _read_ndc_11_filetype_7(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    rec = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<ii16sb12s', bytes[132:-5]):
            [Cycle, Step_Index, Status] = [i[0], i[1], i[3]]
            if Step_Index != 0:
                rec.append([Cycle+1, Step_Index, state_dict[Status]])

    # 创建 DataFrame
    df = pd.DataFrame(rec, columns=['Cycle', 'Step_Index', 'Status'])
    df['Step'] = df.index + 1
    return df


def _read_ndc_11_filetype_18(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    rec = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<isffff12siiih', bytes[132:-63]):
            Time = i[0]
            [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
            [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
            [Timestamp, Step, Index] = [i[7], i[8], i[9]]
            Msec = i[10]
            if Index != 0:
                rec.append([Time/1000,
                            Charge_Capacity/3600, Discharge_Capacity/3600,
                            Charge_Energy/3600, Discharge_Energy/3600,
                            datetime.fromtimestamp(Timestamp + Msec/1000, timezone.utc), Step, Index])

    # 创建 DataFrame
    df = pd.DataFrame(rec, columns=[
        'Time',
        'Charge_Capacity(mAh)', 'Discharge_Capacity(mAh)',
        'Charge_Energy(mWh)', 'Discharge_Energy(mWh)',
        'Timestamp', 'Step', 'Index']).astype({'Time': 'float'})
    df['Step'] = _count_changes(df['Step'])

    # 将时间戳转换为本地时区
    tz = datetime.now().astimezone().tzinfo
    df['Timestamp'] = df['Timestamp'].dt.tz_convert(tz)

    return df


def _read_ndc_14_filetype_1(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    rec = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<ff', bytes[132:-4]):
            if (i[0] != 0):
                rec.append([i[0], 1000*i[1]])

    # 创建 DataFrame
    df = pd.DataFrame(rec, columns=['Voltage', 'Current(mA)'])
    df['Index'] = df.index + 1
    return df


def _read_ndc_14_filetype_5(mm):
    record_len = 4096
    header = 4096

    # 读取数据记录
    aux = []
    mm.seek(header)
    while mm.tell() < mm.size():
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<f', bytes[132:-4]):
            aux.append(i[0])

    # 创建 DataFrame
    aux_df = pd.DataFrame(aux, columns=['T'])
    aux_df['Index'] = aux_df.index + 1

    return aux_df


def _read_ndc_14_filetype_7(mm):
    return _read_ndc_11_filetype_7(mm)


def _read_ndc_14_filetype_18(mm):
    mm_size = mm.size()
    record_len = 4096
    header = 4096

    # 读取数据记录
    rec = []
    mm.seek(header)
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        for i in struct.iter_unpack('<isffff12siiih8s', bytes[132:-59]):
            Time = i[0]
            [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
            [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
            [Timestamp, Step, Index] = [i[7], i[8], i[9]]
            Msec = i[10]
            if Index != 0:
                rec.append([Time/1000,
                            Charge_Capacity*1000, Discharge_Capacity*1000,
                            Charge_Energy*1000, Discharge_Energy*1000,
                            datetime.fromtimestamp(Timestamp + Msec/1000, timezone.utc), Step, Index])

    # 创建 DataFrame
    df = pd.DataFrame(rec, columns=[
        'Time',
        'Charge_Capacity(mAh)', 'Discharge_Capacity(mAh)',
        'Charge_Energy(mWh)', 'Discharge_Energy(mWh)',
        'Timestamp', 'Step', 'Index']).astype({'Time': 'float'})
    df['Step'] = _count_changes(df['Step'])

    # 将时间戳转换为本地时区
    tz = datetime.now().astimezone().tzinfo
    df['Timestamp'] = df['Timestamp'].dt.tz_convert(tz)

    return df


def _bytes_to_list_ndc(bytes):
    """解释 ndc 字节字符串的辅助函数"""

    # 从字节字符串中提取字段
    [Index, Cycle, Step, Status] = struct.unpack('<IIBB', bytes[8:18])
    [Time, Voltage, Current] = struct.unpack('<Qii', bytes[23:39])
    [Charge_capacity, Discharge_capacity,
     Charge_energy, Discharge_energy] = struct.unpack('<qqqq', bytes[43:75])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', bytes[75:82])
    [Range] = struct.unpack('<i', bytes[82:86])

    multiplier = multiplier_dict[Range]

    # 创建记录
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
    """解释辅助记录的辅助函数"""
    [Aux] = struct.unpack('<B', bytes[3:4])
    [Index] = struct.unpack('<I', bytes[8:12])
    [T] = struct.unpack('<h', bytes[41:43])
    [V] = struct.unpack('<i', bytes[31:35])

    return [Index, Aux, V/10000, T/10]


def _aux_bytes_74_to_list_ndc(bytes):
    """解释辅助记录的辅助函数"""
    [Aux] = struct.unpack('<B', bytes[3:4])
    [Index] = struct.unpack('<I', bytes[8:12])
    [V] = struct.unpack('<i', bytes[31:35])
    [T, t] = struct.unpack('<hh', bytes[41:45])

    return [Index, Aux, V/10000, T/10, t/10]
