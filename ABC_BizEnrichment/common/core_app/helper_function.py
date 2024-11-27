import inspect
from django.urls import path
from django.utils.text import slugify

from core_app.models import LicenseNumber

class CSVImportAdminMixin:
    """
    Mixin to handle custom merge URL routing and dynamic changelist context.
    """
    csv_import_url_name = None  # Define in subclasses
    print('csv_import_url_name')
    def get_urls(self):
        # Your existing URL handling code
        custom_urls = [
            path(f"{self.csv_import_url_name}-csv/", self.get_import_view(), name=f"{self.csv_import_url_name}_csv"),
        ]
        return custom_urls + super().get_urls()
    def get_import_view(self):
        """
        Returns the actual merge view function from the subclass.
        Subclasses should define this method.
        """
        raise NotImplementedError("Subclasses must define 'get_import_view'.")

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context[f"{self.csv_import_url_name}_csv_url"] = f"admin:{self.csv_import_url_name}_csv"
        return super().changelist_view(request, extra_context=extra_context)
    

def get_or_create_license_number(license_number):
    """Gets or creates a LicenseNumber object based on the provided license number."""
    try:
        license_number = int(license_number)
        license_number_obj, _ = LicenseNumber.objects.get_or_create(license_number=license_number)
        return license_number_obj
    except (ValueError, TypeError):
        return None
    

def import_csv_data(request, model, data_mapping_function):
    full_function_name = get_full_function_name()
    if request.method == "POST":
        form = CSVImportForm(request.POST, request.FILES)
        csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
        reader = csv.DictReader(csv_file)
        try:
            batch_size = getattr(settings, 'BATCH_SIZE', 1000)
            rows = list(reader)  # Convert to list for easy batching
            total_rows = len(rows)
            for i in range(0, total_rows, batch_size):
                batch = rows[i:i + batch_size]  # Get the current batch
                for row in batch:
                    try:
                        data_mapping_function(row)  # Process each row with a custom function
                        logger.info(f"{full_function_name}: Processed {len(batch)} records (batch {i // batch_size + 1}).")
                    except Exception as e:
                        logger.error(f"{full_function_name}: Error processing record: {str(e)}")
                time.sleep(10)
        except Exception as e:
            logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
            model.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)

    form = CSVImportForm()
    context = {
        **model.admin_site.each_context(request),
        "opts": model._meta,
        "form": form,
    }
    return TemplateResponse(request, "admin/csv_form.html", context)