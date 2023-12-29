# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [v2024.01.02]
### Added
- Support for nda files from BTS9.
- New 'cycle_mode' keyword argument to specify how cycle number should be generated.
- Additional current ranges.

### Fixed
- Bug fixes for ndax support.


## [v2023.12.01]
### Added
- Support for ndax from BTS Server 8.
- Additional ndax verison information now returned as logging.

### Changed
- 'software_cycle_number=True' is now the default for ndax.


## [v2023.11.01]
### Added
- Ability to read auxiliary data from ndax files.
- pytest regression tests for code development.

### Fixed
- Support for constant power charge and discharge.
- Additional current ranges.

### Changed
- 'software_cycle_number=True' is one again the default behavior for nda.


## [v2023.10.02]
### Added
- Support for constant power charge and discharge.
- Additional current ranges.


## [v2023.07.03]
### Added
- Support for reading ndax.


## [v2023.06.01]
### Added
- Missing current range.
- Performance improvements

### Changed
- The cycle number is now read directly from the nda file for compatibility with the newest version of BTSDA. Previous numbering can be restored with the new 'software_cycle_number' flag.


## [v2023.05.01]
### Fixed
- Issue #20, files failing to load.
- Updates to README.


## [v2023.04.14]
### Added
- Significant performance improvement.
- PEP8 code formatting consistency.

### Fixed
- Issue #22 that occasionally resulted in incorrect temperature values.


## [v2023.02.01]
### Added
- Support for additional current ranges and states

### Fixed
- Maintenance updates and code cleanup

### Changed
- Conditions used for locating first data record


## [v2022.10.03]
### Added
- Additional current ranges

### Fixed
- Improved performance and reduced memory usage.


## [v2022.09.06]
### Added
- Commandline nda file conversion tool
- Performance improvements

### Fixed
- Added missing status and current ranges

### Changed
- 'Jump' field removed
- Default index is used for the DataFrame and set_index() is no longer called


## [v2022.08.01]
### Added
- Performance improvements

### Fixed
- Added missing status and current range


## [v2022.07.01]
### Added
- Ability to read temperature fields

### Fixed
- Step field now matches Neware output
- Correct handling of 'SIM' steps

### Changed
- Charge/discharge capacity/energy are now separate fields


## [2022.06.01]
### Added
- More robust error handling
- Additional hardware ranges implemented

### Fixed
- Cycle and Step now match Neware


## [v0.1.0] - 2022-05-10
### Added
- Initial release tested on nda versions 7.6 and 8.0.