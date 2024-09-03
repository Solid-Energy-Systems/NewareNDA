import os.path as osp
import glob


def pytest_addoption(parser):
    parser.addoption(
        "--ndaDir",
        action="store",
        default=osp.join(osp.dirname(osp.abspath(__file__)), 'nda'),
        help="The directory with the modified test results"
    )
    parser.addoption(
        "--refDir",
        action="store",
        default=osp.join(osp.dirname(osp.abspath(__file__)), 'reference'),
        help="The directory with the base test results"
    )
    parser.addoption(
        "--no_software_cycle_number",
        action="store_false",
        help="Do not generate the cycle number"
    )
    parser.addoption(
        "--cycle_mode",
        action="store",
        default='chg',
        help="Selects how the cycle is incremented."
    )


def pytest_generate_tests(metafunc):
    nda_dir = metafunc.config.getoption('--ndaDir')
    ref_dir = metafunc.config.getoption('--refDir')
    software_cycle_number = metafunc.config.getoption('--no_software_cycle_number')
    cycle_mode = metafunc.config.getoption('--cycle_mode')

    # Generate list of files to compare
    nda_files = glob.glob(nda_dir + '/**/*.nda*', recursive=True)
    ref_files = [osp.join(ref_dir, f"{osp.splitext(osp.basename(f))[0]}.ftr")
                 for f in nda_files]
    cycle_modes = [cycle_mode for f in nda_files]
    software_cycle_numbers = [software_cycle_number for f in nda_files]

    metafunc.parametrize(
        "nda_file, ref_file, software_cycle_number, cycle_mode",
        list(zip(nda_files, ref_files, software_cycle_numbers, cycle_modes)))
