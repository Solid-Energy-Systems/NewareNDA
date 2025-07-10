# © 2022-2024 版权所有 SES AI
# 作者：Daniel Cogswell
# 邮箱：danielcogswell@ses.ai

import os
import mmap
import struct
import logging
from datetime import datetime, timezone
import pandas as pd
import re

from .utils import _generate_cycle_number, _count_changes
from .dicts import rec_columns, dtype_dict, aux_dtype_dict, state_dict, \
    multiplier_dict
from .NewareNDAx import read_ndax

logger = logging.getLogger('newarenda')


def _decode_remarks(byte_data):
    """
    智能解码备注字节数据，支持多种编码格式
    
    参数：
        byte_data: 要解码的字节数据
    
    返回：
        str: 解码后的字符串，如果所有编码都失败则返回空字符串
    """
    # 常见编码格式列表，按优先级排序
    encodings = ['utf-8', 'gb2312', 'gbk', 'ascii']
    
    for encoding in encodings:
        try:
            remarks = byte_data.decode(encoding)
            # 清除空字符并去除首尾空白
            remarks = remarks.replace(chr(0), '').strip()
            if remarks:  # 如果解码成功且不为空
                logger.info(f"Remarks (使用 {encoding} 编码): {remarks}")
                return remarks
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # 最后尝试latin1，但要检查是否包含可打印字符
    try:
        remarks = byte_data.decode('latin1')
        remarks = remarks.replace(chr(0), '').strip()
        # 检查是否主要由可打印ASCII字符组成
        if remarks and any(32 <= ord(c) <= 126 for c in remarks):
            # 检查可打印字符的比例
            printable_ratio = sum(1 for c in remarks if 32 <= ord(c) <= 126) / len(remarks)
            if printable_ratio >= 0.8:  # 如果80%以上是可打印字符
                logger.info(f"Remarks (使用 latin1 编码): {remarks}")
                return remarks
    except (UnicodeDecodeError, UnicodeError):
        pass
    
    # 如果所有编码都失败或解码结果不合理
    logger.warning("无法使用任何编码格式解析备注字节数据")
    return ""


def read(file, software_cycle_number=True, cycle_mode='chg', log_level='INFO'):
    """
    从 Neware nda 或 ndax 二进制文件中读取电化学数据。

    参数：
        file (str)：要读取的 .nda 或 .ndax 文件名
        software_cycle_number (bool)：重新生成循环编号以匹配
            Neware 的"充电优先"循环统计设置
        cycle_mode (str)：选择循环递增方式。
            'chg': (默认) 在放电后以充电步骤设置新循环。
            'dchg': 在充电后以放电步骤设置新循环。
            'auto': 将第一个非静置状态识别为递增状态。
        log_level (str)：设置模块的日志级别。默认值："INFO"
            选项："CRITICAL"、"ERROR"、"WARNING"、"INFO"、"DEBUG"、"NOTSET"
    返回：
        df (pd.DataFrame)：包含文件中所有记录的 DataFrame
    """

    # 设置日志
    log_level = log_level.upper()
    if log_level in logging._nameToLevel.keys():
        logger.setLevel(log_level)
    else:
        logger.warning(f"Logging level '{log_level}' not supported; Defaulting to 'INFO'. "
                       f"Supported options are: {', '.join(logging._nameToLevel.keys())}")

    # 识别文件类型并相应处理
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
    从 Neware nda 二进制文件中读取电化学数据的函数。

    参数：
        file (str)：要读取的 .nda 文件名
        software_cycle_number (bool)：生成循环编号字段以匹配
            旧版本的 BTSDA
        cycle_mode (str)：选择循环递增方式。
            'chg': (默认) 在放电后以充电步骤设置新循环。
            'dchg': 在充电后以放电步骤设置新循环。
            'auto': 将第一个非静置状态识别为递增状态。
    返回：
        df (pd.DataFrame)：包含文件中所有记录的 DataFrame
    """
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        if mm.read(6) != b'NEWARE':
            logger.error(f"{file} does not appear to be a Neware file.")
            raise ValueError(f"{file} does not appear to be a Neware file.")

        # 获取文件版本
        [nda_version] = struct.unpack('<B', mm[14:15])
        logger.info(f"NDA version: {nda_version}")

        # 尝试查找服务器和客户端版本信息
        version_loc = mm.find(b'BTSServer')
        if version_loc != -1:
            mm.seek(version_loc)
            server = mm.read(50).strip(b'\x00').decode()
            logger.info(f"Server: {server}")
            mm.seek(50, 1)
            client = mm.read(50).strip(b'\x00').decode()
            logger.info(f"Client: {client}")
        else:
            logger.info("未找到 BTS 版本！")

        # 版本特定设置
        # print(f"nda_version == {nda_version}")
        if nda_version == 8: # 8 基于 29 改
            output, aux = _read_nda_8(mm)
        elif nda_version == 22: # 22 基于 29 改
            output, aux = _read_nda_22(mm)
        elif nda_version == 23: # 23 基于 22 改
            output, aux = _read_nda_23(mm)
        elif nda_version == 26: # 26 和 29 相同
            output, aux = _read_nda_26(mm)
        elif nda_version == 28: # 28 和 29 相同
            output, aux = _read_nda_29(mm)
        elif nda_version == 29: # 原始版本
            output, aux = _read_nda_29(mm)
        elif nda_version == 130: # 原始版本
            output, aux = _read_nda_130(mm)
        else:
            logger.error(f"不支持 nda 版本 {nda_version}！")
            raise NotImplementedError(f"不支持 nda 版本 {nda_version}！")

    # 创建 DataFrame 并按索引排序
    df = pd.DataFrame(output, columns=rec_columns)
    df.drop_duplicates(subset='Index', inplace=True)

    if not df['Index'].is_monotonic_increasing:
        df.sort_values('Index', inplace=True)

    df.reset_index(drop=True, inplace=True)

    # 连接温度数据
    aux_df = pd.DataFrame(aux, columns=['Index', 'Aux', 'T', 'V'])
    aux_df.drop_duplicates(inplace=True)
    if not aux_df.empty:
        aux_df = aux_df.astype(
            {k: aux_dtype_dict[k] for k in aux_dtype_dict.keys() & aux_df.columns})
        pvt_df = aux_df.pivot(index='Index', columns='Aux')
        pvt_df.columns = pvt_df.columns.map(lambda x: ''.join(map(str, x)))
        df = df.join(pvt_df, on='Index')

    # 后处理
    df['Step'] = _count_changes(df['Step'])

    # v8 文件补充容量与能量（通过数值积分获得）
    if nda_version == 8:
        # Δt 处理：遇到新工步时 Time 会重置，需要将负差值替换为当前 Time
        dt = df['Time'].diff()
        dt.iloc[0] = 0
        dt = dt.where(dt >= 0, df['Time'])

        # 绝对时间戳
        if df['Timestamp'].notnull().any():
            start_ts = df['Timestamp'].iloc[0]
            df['Timestamp'] = start_ts + pd.to_timedelta(dt.cumsum(), unit='s')

        # mAh 增量
        cap_inc = (df['Current(mA)'].abs()) * dt / 3600

        # 充/放电容量累加
        df['Charge_Capacity(mAh)'] = (cap_inc.where(df['Current(mA)'] > 0, 0)).cumsum()
        df['Discharge_Capacity(mAh)'] = (cap_inc.where(df['Current(mA)'] < 0, 0)).cumsum()

        # 能量增量 (mWh)
        en_inc = df['Voltage'] * cap_inc
        df['Charge_Energy(mWh)'] = (en_inc.where(df['Current(mA)'] > 0, 0)).cumsum()
        df['Discharge_Energy(mWh)'] = (en_inc.where(df['Current(mA)'] < 0, 0)).cumsum()

    if software_cycle_number:
        df['Cycle'] = _generate_cycle_number(df, cycle_mode)
    df = df.astype(dtype=dtype_dict)

    return df, nda_version


def _read_nda_8(mm):
    """nda 版本 8 的辅助函数。

    与后续版本结构差异较大，其记录长度仅 59 B，
    但仍以 0x55 0x00 开头，无显式页眉标识。此处使用以下经验解析：

    字节偏移（基于经验文件）：
    0–1   : 0x55 0x00 固定帧头
    2–5   : Index (uint32)
    10    : Step (uint8)
    18    : Status (uint8)
    20–23 : Current (int32, μA)
    24–27 : Voltage (int32, 1e-4 V)
    其余字段留空或暂未解析。
    """

    mm_size = mm.size()

    # 活性物质质量与备注区在旧版文件中不存在或无法可靠读取，此处跳过。

    record_len = 59

    # 在文件任意位置搜索第一个 0x55 0x00 作为起始记录。
    header = -1
    search_pos = 0
    while True:
        cand = mm.find(b'\x55\x00', search_pos)
        if cand == -1 or cand + record_len >= mm_size:
            break
        # 检查下一条 0x55 0x00 是否正好相隔 59 字节
        if mm[cand + record_len: cand + record_len + 2] == b'\x55\x00':
            header = cand
            break
        search_pos = cand + 1

    if header == -1:
        logger.error("未找到满足 59 B 间隔模式的记录头，无法解析 nda v8 文件！")
        raise EOFError("File does not contain any valid records.")

    mm.seek(header)

    output = []
    idx = 1  # 自增索引作为 Index 的后备方案

    # --------------------
    # 提取开始时间戳（ASCII 字符串）
    # 典型格式："YYYY.MM.DD HH:MM:SS"
    # --------------------
    start_ts = None
    m = re.search(rb'20[0-9]{2}\.[0-9]{2}\.[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}', mm[:header])
    if m:
        try:
            ts_str = m.group().decode()
            start_ts = datetime.strptime(ts_str, "%Y.%m.%d %H:%M:%S")
            # 转换为本地时区
            start_ts = start_ts.replace(tzinfo=timezone.utc).astimezone()
        except Exception:
            start_ts = None

    while mm.tell() + record_len <= mm_size:
        chunk = mm.read(record_len)

        # 记录必须以 0x55 0x00 开头
        if chunk[0:2] != b'\x55\x00':
            break

        rec = _bytes_to_list_8(chunk, fallback_index=idx)
        if rec:
            # 计算绝对时间戳
            if start_ts is not None:
                try:
                    rec[11] = start_ts + pd.to_timedelta(rec[4], unit='s')
                except Exception:
                    pass
            output.append(rec)
        idx += 1

    # v8 暂未发现辅助温度记录
    aux = []
    return output, aux


def _bytes_to_list_8(bytes, fallback_index=0):
    """解析 nda v8 版 59 B 数据记录，返回符合 rec_columns 的列表。"""

    # v8 文件中的 Index 字段通常为 0，因此直接使用回退索引
    Index = fallback_index

    # 状态代码位于字节 18；将其直接用作"工步号"，后续由 _count_changes 重新编号
    Status_code = bytes[18]
    Step = Status_code  # 工步号基于状态变化，而非记录计数

    # v8 版记录格式（经反向工程）
    # 20–23 : Time (int32, s)
    # 24–27 : Voltage (int32, 1e-4 V)
    # 28–31 : Current (int32, µA)

    Time = struct.unpack('<i', bytes[20:24])[0]
    Voltage_raw = struct.unpack('<i', bytes[24:28])[0]
    Current_raw = struct.unpack('<i', bytes[28:32])[0]

    Voltage = Voltage_raw / 10000            # 转换为 V
    Current = Current_raw / 1000             # 转换为 mA

    # 占位符：容量与能量稍后在 read_nda() 中根据电流积分计算
    Charge_capacity = Discharge_capacity = 0.0
    Charge_energy = Discharge_energy = 0.0

    # v8 文件通常仅包含单循环
    Cycle = 1

    # 自定义 v8 状态映射。若未包含则回退到通用 state_dict
    v8_state_dict = {
        1: 'Rest',
        2: 'CC_DChg',
        3: 'CV_DChg',
        4: 'CC_DChg',
    }
    Status = v8_state_dict.get(Status_code, state_dict.get(Status_code, f'Unknown_{Status_code}'))

    # 时间戳占位；稍后在 _read_nda_8 中使用开始时间计算
    timestamp = None

    return [
        Index,
        Cycle,
        Step,
        Status,
        Time,
        Voltage,
        Current,
        Charge_capacity,
        Discharge_capacity,
        Charge_energy,
        Discharge_energy,
        timestamp
    ]


def _read_nda_22(mm):
    """nda 版本 22 的辅助函数 (暂时与版本 29 相同)"""
    mm_size = mm.size()

    # 获取活性物质质量
    [active_mass] = struct.unpack('<I', mm[152:156])
    logger.info(f"Active mass: {active_mass/1000} mg")

    # 使用智能解码函数处理备注
    remarks = _decode_remarks(mm[2317:2417])

    # 识别数据部分的开头
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

    # 读取数据记录
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # 检查数据记录
            if (bytes[0:2] == b'\x55\x00'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                output.append(_bytes_to_list_22(bytes))

            # 检查辅助记录
            elif (bytes[0:1] == b'\x65'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                aux.append(_aux_bytes_to_list(bytes))

    return output, aux

def _read_nda_23(mm):
    """nda 版本 23 的辅助函数"""
    mm_size = mm.size()

    # 获取活性物质质量
    [active_mass] = struct.unpack('<I', mm[152:156])
    logger.info(f"Active mass: {active_mass/1000} mg")

    # 使用智能解码函数处理备注
    remarks = _decode_remarks(mm[2317:2417])

    # 识别数据部分的开头
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

    # 读取数据记录
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # 检查数据记录
            if (bytes[0:2] == b'\x55\x00'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                output.append(_bytes_to_list_23(bytes))

            # 检查辅助记录
            elif (bytes[0:1] == b'\x65'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                aux.append(_aux_bytes_to_list(bytes))

    return output, aux


def _read_nda_26(mm):
    """nda 版本 26 的辅助函数"""
    mm_size = mm.size()

    # 获取活性物质质量
    [active_mass] = struct.unpack('<I', mm[152:156])
    logger.info(f"Active mass: {active_mass/1000} mg")

    # 使用智能解码函数处理备注
    remarks = _decode_remarks(mm[2317:2417])

    # 识别数据部分的开头
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

    # 读取数据记录
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # 检查数据记录
            if (bytes[0:2] == b'\x55\x00'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                output.append(_bytes_to_list(bytes))

            # 检查辅助记录
            elif (bytes[0:1] == b'\x65'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                aux.append(_aux_bytes_to_list(bytes))

    return output, aux


def _read_nda_29(mm):
    """nda 版本 29 的辅助函数"""
    mm_size = mm.size()

    # 获取活性物质质量
    [active_mass] = struct.unpack('<I', mm[152:156])
    logger.info(f"Active mass: {active_mass/1000} mg")

    # 使用智能解码函数处理备注
    remarks = _decode_remarks(mm[2317:2417])

    # 识别数据部分的开头
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

    # 读取数据记录
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # 检查数据记录
            if (bytes[0:2] == b'\x55\x00'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                output.append(_bytes_to_list(bytes))

            # 检查辅助记录
            elif (bytes[0:1] == b'\x65'
                    and bytes[82:87] == b'\x00\x00\x00\x00'):
                aux.append(_aux_bytes_to_list(bytes))

    return output, aux


def _read_nda_130(mm):
    """nda 版本 130 的辅助函数"""
    mm_size = mm.size()

    # 识别数据部分的开头
    record_len = 88
    identifier = mm[1024:1030]
    if mm[1024:1025] == b'\x55':  # BTS 9.1
        # Find next record and get length
        record_len = mm.find(mm[1024:1026], 1026) - 1024
    mm.seek(1024)

    # 读取数据记录
    output = []
    aux = []
    while mm.tell() < mm_size:
        bytes = mm.read(record_len)
        if len(bytes) == record_len:

            # 检查数据记录
            if bytes[0:1] == b'\x55':
                output.append(_bytes_to_list_BTS91(bytes))
                if record_len == 56:
                    aux.append(_aux_bytes_to_list_BTS91(bytes))
            elif bytes[0:6] == identifier:
                output.append(_bytes_to_list_BTS9(bytes[4:]))

            # 检查辅助记录
            elif bytes[0:5] == b'\x00\x00\x00\x00\x65':
                aux.append(_aux_bytes_to_list(bytes[4:]))

            elif bytes[0:1] == b'\x81':
                break

    # 查找页脚数据块
    footer = mm.rfind(b'\x06\x00\xf0\x1d\x81\x00\x03\x00\x61\x90\x71\x90\x02\x7f\xff\x00', 1024)
    if footer != -1:
        mm.seek(footer+16)
        bytes = mm.read(499)

        # 获取活性物质质量
        [active_mass] = struct.unpack('<d', bytes[-8:])
        logger.info(f"Active mass: {active_mass} mg")

        # 使用智能解码函数处理备注
        remarks = _decode_remarks(bytes[363:491])

    return output, aux


def _valid_record(bytes):
    """识别有效记录的辅助函数"""
    # 检查非零状态
    [Status] = struct.unpack('<B', bytes[12:13])
    return (Status != 0)


def _bytes_to_list(bytes):
    """解释字节字符串的辅助函数"""

    # 从字节字符串中提取字段
    [Index, Cycle, Step] = struct.unpack('<III', bytes[2:14])
    [Status, Jump, Time, Voltage, Current] = struct.unpack('<BBQii', bytes[12:30])
    [Charge_capacity, Discharge_capacity,
     Charge_energy, Discharge_energy] = struct.unpack('<qqqq', bytes[38:70])
    [Y, M, D, h, m, s] = struct.unpack('<HBBBBB', bytes[70:77])
    [Range] = struct.unpack('<i', bytes[78:82])

    # 索引不应为零
    if Index == 0 or Status == 0:
        return []

    multiplier = multiplier_dict[Range]

    # 为记录创建字典
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
    """解释 BTS9 字节字符串的辅助函数"""
    [Step, Status] = struct.unpack('<BB', bytes[5:7])
    [Index] = struct.unpack('<I', bytes[12:16])
    [Time, Voltage, Current] = struct.unpack('<Qff', bytes[24:40])
    [Charge_Capacity, Charge_Energy,
     Discharge_Capacity, Discharge_Energy,
     Date] = struct.unpack('<ffffQ', bytes[48:72])

    # 为记录创建字典
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
    """解释 BTS9.1 字节字符串的辅助函数"""
    [Step, Status] = struct.unpack('<BB', bytes[2:4])
    [Index, Time, Time_ns] = struct.unpack('<III', bytes[8:20])
    [Current, Voltage, Capacity, Energy] = struct.unpack('<ffff', bytes[20:36])
    [Date, Date_ns] = struct.unpack('<II', bytes[44:52])

    # 将容量和能量转换为充电和放电字段
    Charge_Capacity = 0 if Capacity < 0 else Capacity
    Discharge_Capacity = 0 if Capacity > 0 else abs(Capacity)
    Charge_Energy = 0 if Energy < 0 else Energy
    Discharge_Energy = 0 if Energy > 0 else abs(Energy)

    # 为记录创建字典
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
    """解释辅助记录的辅助函数"""
    [Aux, Index] = struct.unpack('<BI', bytes[1:6])
    [V] = struct.unpack('<i', bytes[22:26])
    [T] = struct.unpack('<h', bytes[34:36])

    return [Index, Aux, T/10, V/10000]


def _aux_bytes_to_list_BTS91(bytes):
    [Index] = struct.unpack('<I', bytes[8:12])
    [T] = struct.unpack('<f', bytes[52:56])
    return [Index, 1, T, None]


def _bytes_to_list_22(bytes):
    """解析 nda version 22 数据记录

    该版本与较新的 29 版数据结构大体相同，但存在以下差异：
    1. Step 字段仅占 2 字节（uint16），紧跟在 Cycle 之后。
    2. 日期时间以 Unix epoch（秒）+ 毫秒存储，分别为 4 字节和 2 字节，
       位于原年份等字段所在位置。
    3. Range 字段向后顺延 2 字节至偏移 80–83。
    """
    
    # v22 版本专用的 multiplier 字典，Range 为 0 时使用 1e-2 缩放
    multiplier_dict_v22 = multiplier_dict.copy()
    multiplier_dict_v22[0] = 1e-2

    # 基本字段解析
    Index, Cycle = struct.unpack('<II', bytes[2:10])
    Step,         = struct.unpack('<H',  bytes[10:12])
    Status, Jump  = struct.unpack('<BB', bytes[12:14])

    # 时间、电压、电流
    Time, Voltage, Current = struct.unpack('<Qii', bytes[14:30])

    # 容量 / 能量（int64）
    Charge_capacity, Discharge_capacity, Charge_energy, Discharge_energy = \
        struct.unpack('<qqqq', bytes[38:70])

    # 时间戳（秒）+ 毫秒
    Timestamp_sec, = struct.unpack('<I', bytes[70:74])
    # bytes[74:78] 似乎为保留字节（全 0）
    Msec,          = struct.unpack('<H', bytes[78:80])

    # Range
    Range,         = struct.unpack('<i', bytes[80:84])

    # 无效索引或静置状态跳过
    if Index == 0 or Status == 0:
        return []

    multiplier = multiplier_dict_v22[Range]

    # 生成本地时区时间戳
    ts = datetime.fromtimestamp(Timestamp_sec + Msec/1000, timezone.utc).astimezone()

    rec = [
        Index,
        Cycle + 1,
        Step,
        state_dict.get(Status, f'Unknown_{Status}'),
        Time/1000,
        Voltage/10000,
        Current*multiplier,
        Charge_capacity*multiplier/3600,
        Discharge_capacity*multiplier/3600,
        Charge_energy*multiplier/3600,
        Discharge_energy*multiplier/3600,
        ts
    ]

    return rec

def _bytes_to_list_23(bytes):
    """解析 nda version 23 数据记录

    该版本与 v22 版数据结构大体相同，但 Range=0 时的电流缩放系数不同。
    1. Step 字段仅占 2 字节（uint16），紧跟在 Cycle 之后。
    2. 日期时间以 Unix epoch（秒）+ 毫秒存储，分别为 4 字节和 2 字节，
       位于原年份等字段所在位置。
    3. Range 字段向后顺延 2 字节至偏移 80–83。
    """
    
    # v23 版本专用的 multiplier 字典，Range 为 0 时使用 1e-3 缩放
    multiplier_dict_v23 = multiplier_dict.copy()
    multiplier_dict_v23[0] = 1e-3

    # 基本字段解析
    Index, Cycle = struct.unpack('<II', bytes[2:10])
    Step,         = struct.unpack('<H',  bytes[10:12])
    Status, Jump  = struct.unpack('<BB', bytes[12:14])

    # 时间、电压、电流
    Time, Voltage, Current = struct.unpack('<Qii', bytes[14:30])

    # 容量 / 能量（int64）
    Charge_capacity, Discharge_capacity, Charge_energy, Discharge_energy = \
        struct.unpack('<qqqq', bytes[38:70])

    # 时间戳（秒）+ 毫秒
    Timestamp_sec, = struct.unpack('<I', bytes[70:74])
    # bytes[74:78] 似乎为保留字节（全 0）
    Msec,          = struct.unpack('<H', bytes[78:80])

    # Range
    Range,         = struct.unpack('<i', bytes[80:84])

    # 无效索引或静置状态跳过
    if Index == 0 or Status == 0:
        return []

    multiplier = multiplier_dict_v23[Range]

    # 生成本地时区时间戳
    ts = datetime.fromtimestamp(Timestamp_sec + Msec/1000, timezone.utc).astimezone()

    rec = [
        Index,
        Cycle + 1,
        Step,
        state_dict.get(Status, f'Unknown_{Status}'),
        Time/1000,
        Voltage/10000,
        Current*multiplier,
        Charge_capacity*multiplier/3600,
        Discharge_capacity*multiplier/3600,
        Charge_energy*multiplier/3600,
        Discharge_energy*multiplier/3600,
        ts
    ]

    return rec
