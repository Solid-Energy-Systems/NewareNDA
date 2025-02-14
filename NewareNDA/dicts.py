# Names for data fields
rec_columns = [
    'Index', 'Cycle', 'Step', 'Status', 'Time', 'Voltage',
    'Current(mA)', 'Charge_Capacity(mAh)', 'Discharge_Capacity(mAh)',
    'Charge_Energy(mWh)', 'Discharge_Energy(mWh)', 'Timestamp']

# Define precision of fields
dtype_dict = {
    'Index': 'uint32',
    'Cycle': 'uint16',
    'Step': 'uint32',
    'Status': 'category',
    'Time': 'float32',
    'Voltage': 'float32',
    'Current(mA)': 'float32',
    'Charge_Capacity(mAh)': 'float32',
    'Discharge_Capacity(mAh)': 'float32',
    'Charge_Energy(mWh)': 'float32',
    'Discharge_Energy(mWh)': 'float32'
}

aux_dtype_dict = {
    'V': 'float32',
    'T': 'float32',
    't': 'float32'
}

# Dictionary mapping Status integer to string
state_dict = {
    1: 'CC_Chg',
    2: 'CC_DChg',
    3: 'CV_Chg',
    4: 'Rest',
    5: 'Cycle',
    7: 'CCCV_Chg',
    8: 'CP_DChg',
    9: 'CP_Chg',
    10: 'CR_DChg',
    13: 'Pause',
    16: 'Pulse',
    17: 'SIM',
    19: 'CV_DChg',
    20: 'CCCV_DChg',
    21: 'Control',
    22: 'OCV',
    26: 'CPCV_DChg',
    27: 'CPCV_Chg'
}

# Define field scaling based on instrument Range setting
multiplier_dict = {
    -100000000: 10,
    -200000: 1e-2,
    -100000: 1e-2,
    -60000: 1e-2,
    -30000: 1e-2,
    -50000: 1e-2,
    -40000: 1e-2,
    -20000: 1e-2,
    -12000: 1e-2,
    -10000: 1e-2,
    -6000: 1e-2,
    -5000: 1e-2,
    -3000: 1e-2,
    -2000: 1e-2,
    -1000: 1e-2,
    -500: 1e-3,
    -100: 1e-3,
    -50: 1e-4,
    -25: 1e-4,
    -20: 1e-4,
    -10: 1e-4,
    -5: 1e-5,
    -2: 1e-5,
    -1: 1e-5,
    0: 0,
    1: 1e-4,
    2: 1e-4,
    5: 1e-4,
    10: 1e-3,
    20: 1e-3,
    50: 1e-3,
    100: 1e-2,
    200: 1e-2,
    250: 1e-2,
    500: 1e-2,
    1000: 1e-1,
    6000: 1e-1,
    10000: 1e-1,
    12000: 1e-1,
    20000: 1e-1,
    30000: 1e-1,
    40000: 1e-1,
    50000: 1e-1,
    60000: 1e-1,
    100000: 1e-1,
    200000: 1e-1,
}
