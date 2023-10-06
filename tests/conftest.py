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


def pytest_generate_tests(metafunc):
    nda_dir = metafunc.config.getoption('--ndaDir')
    ref_dir = metafunc.config.getoption('--refDir')

    # Generate list of files to compare
    nda_files = glob.glob(nda_dir + '/**/*.nda*', recursive=True)
    ref_files = [osp.join(ref_dir, f"{osp.splitext(osp.basename(f))[0]}.ftr")
                 for f in nda_files]

    metafunc.parametrize("nda_file, ref_file", list(zip(nda_files, ref_files)))
