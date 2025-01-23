from datetime import datetime
import logging
import csv
import time
import csv
from django.db import IntegrityError
from django.urls import path
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
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
        # Get full function name for logging purposes
        full_function_name = get_full_function_name()

        if request.method != "POST":
            return return_response(request, "admin/csv_form.html", context={"opts": self.model._meta, "form": CSVImportForm(), **self.admin_site.each_context(request)})

        # Handle file upload and remove BOM (Byte Order Mark)
        csv_file = remove_bom(request.FILES["csv_file"].file)
        logger.info(f"{full_function_name}: Reading CSV file {request.FILES['csv_file']}...")

        # CSV reading
        reader = csv.DictReader(csv_file)
        batch_size = 1000
        all_records = []
        try:
            rows = list(reader)
            logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {len(rows)}")

            # Loop through the rows and prepare data for bulk insertion
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for row in batch:
                    try:
                        # Prepare data for model creation
                        data = {k: field_mappings[k](row) for k in field_mappings}
                        # Create a new instance for each row
                        instance = model_class(**data)
                        all_records.append(instance)
                        logger.info(f"{full_function_name}: Added record for entity_num {data}.")
                    except Exception as e:
                        logger.error(f"{full_function_name}: Error preparing record data: {str(e)}")
                    # break
                
                # Bulk create once batch is ready
                if all_records:
                    try:
                        model_class.objects.bulk_create(all_records)
                        logger.info(f"{full_function_name}: Imported {len(all_records)} records (batch {i // batch_size + 1}).")
                        all_records.clear()  # Clear the list for the next batch
                    except IntegrityError as e:
                        logger.error(f"{full_function_name}: Integrity error during bulk create: {str(e)}")
                        self.message_user(request, f"Integrity error during bulk import: {str(e)}", messages.ERROR)
                        return None
                    except Exception as e:
                        logger.error(f"{full_function_name}: Error during bulk create: {str(e)}")
                        self.message_user(request, f"Error during bulk import: {str(e)}", messages.ERROR)
                        return None
                # break
                # Optional: Sleep between batches (can be removed or adjusted based on performance)
                time.sleep(1)

            # Success message after processing all batches
            self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
            return HttpResponseRedirect(f"/admin/core_app/{model_class._meta.model_name}/")
        
        except Exception as e:
            logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
            self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return None
        
