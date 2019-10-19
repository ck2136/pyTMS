# pyTMS: Truven MarketScan Analysis python tool

A python package that provides reusable functions for

   1. Extracting cases and controls by ICD9, ICD10, and CPT codes
   2. Extracting clinical, economic and demographic characteristics for the cases and controls.
   3. Setting continuous enrollment criteria (from start of index date of cases)
   4. Conduct HRU + expenditure and medication adherence analysis

# Getting Started

## Installation

### Using pip

If you use `pip`, you can install with:

```
pip install pyTMS
```

### Using `pyTMS`

```
from pyTMS import Study
from pyTMS import Analysis
```

The two main classes implmemented in `pyTMS` is `pyTMS.Study` and `pyTMS.Analysis`.


# Getting help

Please contact [me](mailto:chong.kim@ucdenver.edu) for help.
