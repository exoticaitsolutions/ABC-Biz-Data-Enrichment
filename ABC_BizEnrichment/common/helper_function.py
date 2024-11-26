from datetime import datetime
import inspect
import csv
from io import TextIOWrapper

def safe_parse_date(date_str):
    """
    Safely parses a date string in the format "%d-%b-%y" (e.g. "01-Jan-23") into a datetime.date object.
    If the input string is not a valid date, or is None, this function will return None.
    """
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




def get_model_field_names(model, prefix_filter=None):
    """
    Retrieve all field names for a given model, optionally filtered by a prefix.
    
    Args:
        model (Model): The Django model class (e.g., AgentsInformation).
        prefix_filter (str, optional): Prefix to filter field names (e.g., "merge_data").
    
    Returns:
        list: A list of field names in the model (filtered if prefix_filter is provided).
    """
    # Get all local field names (excluding relations)
    field_names = [field.name for field in model._meta.get_fields() if not field.is_relation]
    
    # Filter by prefix if provided
    if prefix_filter:
        field_names = [name for name in field_names if name.startswith(prefix_filter)]
    
    return field_names