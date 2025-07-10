# 导入neware_reader库用于读取电池测试数据
import sys
import os

# 添加项目根目录到Python路径，以支持包导入
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# 导入NewareNDA包
import NewareNDA as nda

# 测试不同版本的nda文件
# "tests/v_8_22_26/nda/nda_v8.nda" version 8
# "tests/v_8_22_26/nda/nda_v22.nda" version 22  
# "tests/v_8_22_26/nda/nda_v26.nda" version 26
# "tests/v_8_22_26/nda/nda_v28.nda" version 28
PATH = "tests/v_8_22_26/nda/nda_v23.nda"
print(f"正在读取文件: {PATH}")
dataTable, nda_version = nda.read(PATH)
print("数据读取成功！")
print(f"数据形状: {dataTable.shape}")
print(f"数据列: {list(dataTable.columns)}")
# print("\n前5行数据:")
print(dataTable)
print(f"nda版本: {nda_version}")