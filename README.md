# Industrial Ecology Data Commons Tools

***Under early development. May later be merged into [Industrial Ecology Data Commons project](https://github.com/IndEcol/IE_data_commons).***

A collection of tools to interact with the [Industrial Ecology Data Commons project](https://github.com/IndEcol/IE_data_commons). It pareses data input files, validates and uploades them into the database.

## Setup

In order to make this repo work you need access to the Industrial Ecology Freiburg's MySQL database: http://www.database.industrialecology.uni-freiburg.de

Therefore you will need to rename and edit the [`IEDC_paths_TEMPLATE.py`](IEDC_paths_TEMPLATE.py) and [`IEDC_paths_TEMPLATE.py`](IEDC_paths_TEMPLATE.py) files first.

## Content

TODO

## Contact

Author: Niko Heeren (niko.heeren@gmail.com)

Credits: Niko Heeren, Stefan Pauliuk


## TODO

- [ ] Routine to apply for entire directory
- [ ] Walkthrough documentation (maybe jupyter notebook)
- [ ] Function to (chain-) delete classifications from `classification_definitions` *and* `classification_items`

- [x] Routine for data upload
- [x] Function to add user to users table
- [x] Function to add licenses to licenses table
- [x] Routine for creating a custom classification
- [x] Convenience function to pull classification attributes together
- [x] Create repo
- [x] Validation function to check if all aspects exist in the database
- [x] Validation function to check if attributes are present in the DB
- [x] Write basic IO functions
- [x] Make TODO list :)
