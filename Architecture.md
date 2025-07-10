## `NewareNDA` 库架构文档

### 1. 总体架构

`NewareNDA` 是一个用于读取和解析 Neware 电池测试设备生成的二进制数据文件的 Python 库。它主要支持两种文件格式：`.nda` 和 `.ndax`。该库的核心设计思想是将不同文件格式和版本的解析逻辑分离开，同时提供一个统一的入口函数 `NewareNDA.read()` 供用户使用。

其架构可以分为以下几个主要部分：

*   **统一入口 (`__init__.py`, `NewareNDA.py`)**: 用户通过调用 `NewareNDA.read()` 函数来读取文件。这个函数会自动检测文件扩展名（`.nda` 或 `.ndax`），并调用相应的底层解析函数。
*   **`.nda` 文件解析器 (`NewareNDA.py`)**: 专门负责处理旧版的 `.nda` 文件。它内部根据文件版本（如 v29, v130）进一步调用不同的辅助函数来解析二进制数据流。
*   **`.ndax` 文件解析器 (`NewareNDAx.py`)**: 专门负责处理新版的 `.ndax` 文件。`.ndax` 文件本质上是一个 Zip 压缩包，其中包含了多个 `.xml`（元数据）和 `.ndc`（数据）文件。该模块会解压文件，解析元数据，并调用 `.ndc` 解析器来读取记录。
*   **数据字典 (`dicts.py`)**: 包含一系列预定义的字典，用于将原始数据（如状态码、电流范围等）映射为更具可读性的值，并定义了最终输出 DataFrame 中各个字段的数据类型。
*   **工具函数 (`utils.py`)**: 提供了一些跨模块使用的辅助功能，例如根据充放电状态重新生成循环号（Cycle Number）。
*   **命令行接口 (`__main__.py`)**: 使得用户可以直接在终端中使用该库，将 `.nda`/`.ndax` 文件转换为常见的数据格式（如 CSV, Excel 等）。

整个库的工作流程如下：
1.  用户调用 `NewareNDA.read(file_path, ...)`。
2.  函数根据文件扩展名，将任务分派给 `read_nda` 或 `read_ndax`。
3.  解析器打开文件（对于 `.nda` 是直接二进制读取，对于 `.ndax` 是解压缩）。
4.  解析器读取文件头/元数据以获取版本、设备信息等。
5.  解析器根据文件版本和结构，循环读取数据记录。
6.  每个二进制数据记录被送入一个专门的 `_bytes_to_list` 函数，该函数使用 `struct` 模块解包二进制数据，并利用 `dicts.py` 中的字典进行数据转换和缩放。
7.  所有记录被收集起来，转换成一个 Pandas DataFrame。
8.  进行数据后处理，例如合并辅助通道（温度）数据、重新计算步数和循环号。
9.  返回最终的 DataFrame。

### 2. 各模块文件详解

#### `__init__.py`

这是包的入口文件，其作用是向外部暴露核心接口。

*   `from .version import __version__`: 导入版本号，使用户可以 `import NewareNDA; print(NewareNDA.__version__)`。
*   `from .NewareNDA import read`: 导入核心的 `read` 函数，使用户可以直接 `import NewareNDA; NewareNDA.read(...)`，而无需关心该函数具体在哪个子模块中定义。

#### `version.py`

该文件非常简单，只包含一行代码，用于定义库的当前版本号。

*   `__version__ = 'v2025.02.13'`: 定义了 `__version__` 变量。

#### `dicts.py`

此文件定义了整个库中使用的数据字典和类型映射，是数据解析和转换的核心依据。

*   `rec_columns`: 一个列表，定义了最终生成的 DataFrame 的列名顺序。
*   `dtype_dict`: 字典，定义了 DataFrame 中每一列的精确数据类型（如 `uint32`, `float32`, `category`），用于优化内存使用和确保数据一致性。
*   `aux_dtype_dict`: 字典，定义了辅助通道（如温度、电压）数据的数据类型。
*   `state_dict`: 字典，将记录中的数字状态码（如 1, 2, 4）映射为人类可读的字符串（如 `'CC_Chg'`, `'CC_DChg'`, `'Rest'`）。
*   `multiplier_dict`: 字典，定义了不同电流范围（Range）设置下的缩放因子。Neware 文件中的电流和容量值需要乘以这个因子才能得到真实单位（mA, mAh, mWh）的值。

#### `NewareNDA.py`

此模块是解析 `.nda` 格式文件的核心。

*   **`read(file, software_cycle_number=True, cycle_mode='chg', log_level='INFO')`**:
    *   **作用**: 库的主入口函数。
    *   **逻辑**: 它首先设置日志级别，然后检查输入文件的扩展名。如果是 `.nda`，则调用 `read_nda`；如果是 `.ndax`，则调用 `read_ndax`（定义在 `NewareNDAx.py` 中）。
*   **`read_nda(file, software_cycle_number, cycle_mode='chg')`**:
    *   **作用**: 专门读取和解析 `.nda` 文件。
    *   **逻辑**: 使用内存映射（`mmap`）高效地读取文件。首先检查文件头 `b'NEWARE'` 标识，然后读取文件版本。根据版本号（目前支持 `29` 和 `130`），调用对应的私有读取函数（`_read_nda_29` 或 `_read_nda_130`）。获取数据后，它会将原始数据列表转换为 Pandas DataFrame，合并辅助通道数据，并进行后处理（如使用 `_count_changes` 和 `_generate_cycle_number`）。
*   **`_read_nda_29(mm)`** 和 **`_read_nda_130(mm)`**:
    *   **作用**: 针对特定 `.nda` 文件版本的私有辅助函数。
    *   **逻辑**: 它们负责在内存映射的二进制数据中定位数据记录的起始位置，然后逐条读取记录，并将原始字节传递给 `_bytes_to_list` 或 `_aux_bytes_to_list` 进行解析。
*   **`_valid_record(bytes)`**:
    *   **作用**: 检查一个数据记录是否有效（例如，通过检查状态位是否为0）。
*   **`_bytes_to_list(bytes)`**, **`_bytes_to_list_BTS9(bytes)`**, **`_bytes_to_list_BTS91(bytes)`**:
    *   **作用**: 将单条主数据记录的原始字节（bytes）转换为一个数据列表（list）。
    *   **逻辑**: 使用 `struct.unpack` 根据文件格式定义，从字节串中提取各个字段（如索引、循环、电压、电流等）。然后，它会使用 `state_dict` 和 `multiplier_dict` 对原始值进行转换和缩放，并处理时间戳，最后返回一个干净的数据列表。
*   **`_aux_bytes_to_list(bytes)`**, **`_aux_bytes_to_list_BTS91(bytes)`**:
    *   **作用**: 与上一组函数类似，但专门用于解析辅助通道（通常是温度）的记录。

#### `NewareNDAx.py`

此模块负责处理更复杂的 `.ndax` 格式文件。

*   **`read_ndax(file, software_cycle_number=False, cycle_mode='chg')`**:
    *   **作用**: 读取和解析 `.ndax` 文件。
    *   **逻辑**: `.ndax` 是一个 zip 压缩包。此函数使用 `zipfile` 在临时目录中解压文件。它会首先尝试解析 `VersionInfo.xml` 和 `Step.xml` 等元数据文件以获取版本、活性物质质量等信息。核心数据存储在 `.ndc` 文件中。函数会调用 `read_ndc` 来读取 `data.ndc` 以及其他可能存在的数据文件（如 `data_runInfo.ndc`, `data_step.ndc` 和辅助通道的 `data_AUX_...ndc` 文件），然后将所有数据合并成一个总的 DataFrame。
*   **`_data_interpolation(df)`**:
    *   **作用**: 一个重要的数据修复功能。某些 `.ndax` 文件存在数据点缺失问题，此函数通过插值和外推来填补缺失的时间、容量和能量数据，以保证数据的完整性。
*   **`read_ndc(file)`**:
    *   **作用**: 读取单个 `.ndc` 文件的分派函数。
    *   **逻辑**: 它读取 `.ndc` 文件的文件头，以确定其版本（`ndc_version`）和文件类型（`ndc_filetype`）。然后，它动态地查找并调用与该版本和类型匹配的特定解析函数（例如 `_read_ndc_2_filetype_1`）。这种设计使得扩展新 `.ndc` 版本变得容易。
*   **`_read_ndc_*_filetype_*` (e.g., `_read_ndc_2_filetype_1`, `_read_ndc_11_filetype_7`, etc.)**:
    *   **作用**: 一系列高度特化的私有函数，每个函数负责解析一种特定版本和类型的 `.ndc` 文件。
    *   **逻辑**: 它们的实现各不相同，取决于 `.ndc` 文件的内部结构。有些直接解析记录，有些则需要处理复杂的块结构。它们最终都返回一个包含部分数据的 DataFrame。
*   **`_bytes_to_list_ndc(bytes)`**, **`_aux_bytes_65_to_list_ndc(bytes)`**, **`_aux_bytes_74_to_list_ndc(bytes)`**:
    *   **作用**: 类似于 `NewareNDA.py` 中的字节解析函数，但它们是为 `.ndc` 文件中的记录格式量身定制的。它们负责从字节串中解包数据并进行转换。

#### `utils.py`

此模块包含被其他模块复用的通用工具函数。

*   **`_generate_cycle_number(df, cycle_mode='chg')`**:
    *   **作用**: 重新计算“循环号”（Cycle Number）。Neware 软件的循环号计算方式可能与原始文件中记录的不一致。此函数可以模拟 Neware 软件的行为（例如，在放电后的第一个充电步骤将循环号+1）。
    *   **`cycle_mode`**: 参数允许用户指定如何定义一个新循环的开始（`'chg'`：充电开始，`'dchg'`：放电开始，`'auto'`：自动检测）。
*   **`_count_changes(series)`**:
    *   **作用**: 计算一个 Pandas Series 中值的变化次数，并为每个连续的相同值的块分配一个唯一的序号。这通常用于生成“步数”（Step Number）。
*   **`_id_first_state(df)`**:
    *   **作用**: 当 `cycle_mode` 设置为 `'auto'` 时，此函数被调用来确定数据中的第一个有效工步（非“静置”）是充电还是放电，从而决定循环号的递增方式。

#### `__main__.py`

此模块实现了包的命令行接口（CLI），让用户可以方便地在脚本或终端中使用此库进行文件转换。

*   **`main()`**:
    *   **作用**: 程序的入口点。
    *   **逻辑**: 使用 `argparse` 模块来定义和解析命令行参数。用户可以指定输入文件、输出文件、输出格式（如`csv`, `excel`, `feather` 等）、是否重新计算循环号、日志级别等。它调用 `NewareNDA.read()` 来读取数据，然后使用 Pandas 提供的 `to_...` 方法（如 `to_csv`, `to_excel`）将得到的 DataFrame 保存为用户指定的格式。

希望这份详细的文档能帮助您理解 `NewareNDA` 库的内部工作原理和代码结构。