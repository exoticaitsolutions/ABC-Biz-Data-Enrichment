from datetime import datetime
import logging
import csv
import time
import csv
from django.db import IntegrityError
from django.urls import path
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import get_full_function_name, remove_bom, return_response
from django import forms
from django.contrib import admin,messages
from django.conf import settings
from django.http import HttpResponseRedirect

from collections import defaultdict

from ABC_BizEnrichment.common.logconfig import logger

class CSVImportAdminMixin:
    """
    Mixin to handle custom merge URL routing and dynamic changelist context.
    """
    csv_import_url_name = None  # Define in subclasses
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
    



class CSVImportForm(forms.Form):
    csv_file = forms.FileField(label="Upload CSV File", required=True)

class BaseCSVImportAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    def process_csv_import(self, request, model_class, field_mappings):
        if request.method != "POST":
            return return_response(request, "admin/csv_form.html", context={"opts": self.model._meta, "form": CSVImportForm(), **self.admin_site.each_context(request)})

        # Handle file upload and remove BOM (Byte Order Mark)
        csv_file = remove_bom(request.FILES["csv_file"].file)
        
        # CSV reading
        reader = csv.DictReader(csv_file)
        try:
            rows = list(reader)
            total_records = len(rows)
            records_to_create = []  # List to hold instances for bulk_create

            # Use tqdm to show a progress bar while processing each record (optional)
            for row in tqdm(rows, desc="Processing CSV", unit="record", total=total_records):
                try:
                    # Prepare data for model creation
                    data = {k: field_mappings[k](row) for k in field_mappings}
                    # Create a new instance for each row
                    instance = model_class(**data)
                    records_to_create.append(instance)  # Add to list for bulk creation
                except IntegrityError as e:
                    self.message_user(request, f"Integrity error while importing record: {str(e)}", messages.ERROR)
                    return None
                except Exception as e:
                    self.message_user(request, f"Error preparing or saving record: {str(e)}", messages.ERROR)
                    return None

                # Check if we have enough records to insert in bulk
                if len(records_to_create) >= 1000:  # Insert in batches of 1000 (you can adjust the batch size)
                    model_class.objects.bulk_create(records_to_create)
                    records_to_create.clear()  # Clear the list after bulk insert

            # Insert any remaining records (if less than 1000)
            if records_to_create:
                model_class.objects.bulk_create(records_to_create)

            # Success message after processing all records
            self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
            return HttpResponseRedirect(f"/admin/core_app/{model_class._meta.model_name}/")
        
        except Exception as e:
            self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return None
        
