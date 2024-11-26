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
    

