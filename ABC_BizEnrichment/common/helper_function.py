from datetime import datetime
import inspect
import csv
from io import TextIOWrapper

def safe_parse_date(date_str):
    """
    Safely parses a date string in the format "%d-%b-%y" (e.g. "01-Jan-23") into a datetime.date object.
    If the input string is not a valid date, or is None, this function will return None.
    If the input is already a datetime.date object, it will return it directly.
    """
    if isinstance(date_str, datetime):
        # If it's already a datetime object, return it directly
        return date_str.date()  # If you need it as date part only
    if date_str:
        try:
            return datetime.strptime(date_str, "%d-%b-%y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None  

def get_full_function_name():
    """Returns the full class and function name for logging."""
    frame = inspect.currentframe().f_back
    function_name = frame.f_code.co_name
    # Access `self` from local variables
    self_instance = frame.f_locals.get("self", None)
    if self_instance:
        class_name = self_instance.__class__.__name__
    else:
        class_name = "UnknownClass"
    return f"{class_name}.{function_name}"


