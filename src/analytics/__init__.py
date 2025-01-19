"""
This module has all the analytics in individual files.

Usage:
    - preferred way is to use the get_analytics_function(analytics_id)
    - each analytics produce output in two places. destination mentioned in the config and the logger output

Notes:
    - get_analytics_function abstracts the user from the analytics implementation
    - even when the underlying structure of the analytics changes user won't be impacted
    - we can import the function from the individual files as well, but it involves additional level of indirection
    - project level logger is configured in the logging module
    
"""

from ._main import *

