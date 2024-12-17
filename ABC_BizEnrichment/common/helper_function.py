from datetime import datetime
import inspect
from io import TextIOWrapper
from django.db.models import Model

from django.template.response import TemplateResponse
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


def remove_bom(file_obj, encoding="utf-8"):
    """
    Removes the BOM from a file-like object if present and returns the cleaned file object.
    """
    bom = '\ufeff'
    file_obj = TextIOWrapper(file_obj, encoding=encoding, errors='replace')
    first_char = file_obj.read(1)  # Read the first character
    if first_char != bom:
        file_obj.seek(0)  # Reset if no BOM found
    return file_obj

def parse_date(date_str):
    if date_str == "N/A" or not date_str:  # Handle N/A or empty date
        return   # Or return an empty string if you prefer
    else:
        input_format = "%d-%b-%Y"
        input_date = datetime.strptime(date_str, input_format)
        output_format = "%Y-%m-%d"
        output_date_str = input_date.strftime(output_format)
        return output_date_str
    
    
def return_response(request, url, context):
    return TemplateResponse(request, url, context)

def get_column_names(model: Model,exclude_fields, include_relations=False):
    return [
        field.name for field in model._meta.get_fields()
        if (include_relations or not field.is_relation) and field.name not in exclude_fields
    ]