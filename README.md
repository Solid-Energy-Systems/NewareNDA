这是一个 NewareNDA 库的分叉版本。
增加了对 v8, v22, v23, v26, v28 版本的读取。增加了文件解析的鲁棒性。

# NewareNDA

© 2022-2024 版权所有 SES AI
<br>原作者: [Daniel Cogswell](https://github.com/Solid-Energy-Systems/NewareNDA)
<br>邮箱: danielcogswell@ses.ai

用于读取和转换 Neware nda 和 ndax 电池循环文件的 Python 模块和命令行工具。目前两种格式都支持辅助温度字段。

# 安装
从 PyPi 包仓库安装最新版本:
```
pip install --upgrade NewareNDA
```

直接从 Github 安装开发分支:
```
pip install git+https://github.com/jerry328-sudo/NewareNDA.git@master
```

从源代码安装，克隆此仓库并运行:
```
cd NewareNDA
pip install .
```

# 使用
```
import NewareNDA
df = NewareNDA.read('filename.nda')
```

## 日志
额外的测试信息，包括活性物质质量、备注和 BTS 版本，通过 [日志](https://docs.python.org/3/library/logging.html) 返回。以下命令会将此日志信息打印到终端:
```
import logging
logging.basicConfig()
```

## 命令行界面:
```
usage: NewareNDA-cli [-h]
                     [-f {csv,excel,feather,hdf,json,parquet,pickle,stata}]
                     [-s] [-v]
                     [-l {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}]
                     [-c {chg,dchg,auto}]
                     in_file out_file

用于将 Neware NDA 文件转换为其他文件格式的脚本。默认输出格式为 csv。其他格式可能需要安装额外的包。

位置参数:
  in_file               输入文件
  out_file              输出文件

选项:
  -h, --help            显示此帮助消息并退出
  -f {csv,excel,feather,hdf,json,parquet,pickle,stata}, --format {csv,excel,feather,hdf,json,parquet,pickle,stata}
  -s, --no_software_cycle_number
                        生成循环编号字段以匹配旧版本的 BTSDA。
  -v, --version         显示版本
  -l {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}, --log_level {CRITICAL,FATAL,ERROR,WARN,WARNING,INFO,DEBUG,NOTSET}
                        设置 NewareNDA 的日志级别
  -c {chg,dchg,auto}, --cycle_mode {chg,dchg,auto}
                        选择循环递增方式。
```

# 故障排除
如果您遇到密钥错误，通常是您的文件具有我们以前未见过的硬件设置。通常这是一个快速修复，需要将 BTSDA 的输出与 NewareNDA 提取的值进行比较。请启动一个新的 Github Issue，我们将帮助调试。
