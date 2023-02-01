# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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