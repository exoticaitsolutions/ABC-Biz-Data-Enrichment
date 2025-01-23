from datetime import datetime
import inspect
from io import TextIOWrapper
from django.db.models import Model
from django.apps import apps
from django.db import models

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
    """
    Parse a date string into a standardized format ('YYYY-MM-DD'). Handles multiple common date formats.

    Args:
        date_str (str): The date string to be parsed.

    Returns:
        str: The date in 'YYYY-MM-DD' format, or None if the date is invalid or unavailable.
    """
    if date_str == "N/A" or not date_str:  # Handle N/A or empty date
        return None  # Return None to indicate no date
    # List of possible date formats
    input_formats = [
        "%d-%b-%y",
        "%d-%b-%Y",
        "%b %d %Y %I%M%p",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]
    for input_format in input_formats:
        try:
            input_date = datetime.strptime(date_str.strip(), input_format)
            output_date_str = input_date.strftime("%Y-%m-%d")
            return output_date_str
        except ValueError:
            continue
    return None  # Return None if no matching format is found
    
    
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


def generate_model_fields(model_name,app_name):
    model = apps.get_model(app_name, model_name)
    field_definitions = []
    for field in model._meta.get_fields():
        # Skip related fields like ForeignKey or ManyToMany
        if isinstance(field, models.Field):  
            field_name = field.name
            field_type = type(field).__name__
            # Map Python field types to Django's field types (for common fields)
            if field_type == 'CharField':
                field_definitions.append(f"{field_name} = models.CharField(max_length=255, blank=True, null=True)")
            elif field_type == 'TextField':
                field_definitions.append(f"{field_name} = models.TextField(blank=True, null=True)")
            elif field_type == 'IntegerField':
                field_definitions.append(f"{field_name} = models.IntegerField(blank=True, null=True)")
            elif field_type == 'BooleanField':
                field_definitions.append(f"{field_name} = models.BooleanField(default=False)")
            # You can add more mappings for other field types like DateField, FloatField, etc.
            else:
                # For unhandled field types, we'll just return them as is
                field_definitions.append(f"{field_name} = models.{field_type}()")
    return field_definitions



def validate_yelp_rating(rating):
    try:
        # Attempt to convert the rating to a float
        float_rating = float(rating)
        
        # Ensure the rating is within a valid range (e.g., 0 to 5)
        if float_rating < 0 or float_rating > 5:
            raise ValueError("Yelp rating should be between 0 and 5.")
        
        # Return the rounded rating as a string, truncated to 20 characters
        return str(round(float_rating, 2))[:20]  # Truncate to 20 characters
    except (ValueError, TypeError):
        # Log invalid data
        # Ensure it's a valid string that won't cause database issues (empty strings are allowed)
        return "N/A"  # Or any default value like "0.0" or "No rating"