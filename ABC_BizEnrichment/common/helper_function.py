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
        return None  # Return None to indicate no date

    # List of possible date formats
    input_formats = [
        "%d-%b-%y",       # Format like '21-Jun-17'
        "%d-%b-%Y",       # Format like '04-FEB-2021'
        "%b %d %Y %I%M%p",# Format like 'Oct 24 2023  622PM'
        "%Y-%m-%d",       # ISO format like '2013-08-09'
    ]

    for input_format in input_formats:
        try:
            input_date = datetime.strptime(date_str.strip(), input_format)
            output_date_str = input_date.strftime("%Y-%m-%d")
            return output_date_str
        except ValueError:
            continue

    # Log and return None if no format matches
    print(f"Unsupported date format: {date_str}")
    return None
    
    
def return_response(request, url, context):
    return TemplateResponse(request, url, context)

def get_column_names(model: Model,exclude_fields, include_relations=False):
    return [
        field.name for field in model._meta.get_fields()
        if (include_relations or not field.is_relation) and field.name not in exclude_fields
    ]

def get_model_field_definitions(model, exclude_fields=None):
    """
    Returns the field definitions of a model in the format:
    models.<FieldType>(parameters), excluding specified fields.

    Args:
        model (Model): The Django model to introspect.
        exclude_fields (list): List of field names to exclude.

    Returns:
        List[str]: A list of field definitions in the desired format.
    """
    exclude_fields = exclude_fields or []
    field_definitions = []
    for field in model._meta.get_fields():
        if field.name in exclude_fields or not hasattr(field, "get_internal_type"):
            continue  # Skip excluded or non-standard fields
        field_type = field.get_internal_type()
        field_params = []
        # Add max_length if available
        if hasattr(field, "max_length") and field.max_length:
            field_params.append(f"max_length={field.max_length}")
        # Add null if applicable
        if field.null:
            field_params.append("null=True")
        # Add blank if applicable
        if field.blank:
            field_params.append("blank=True")
        # Join parameters
        params_str = ", ".join(field_params)
        field_definition = f"models.{field_type}({params_str})"
        field_definitions.append(f"{field.name}: {field_definition}")
    return field_definitions