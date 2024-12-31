import logging
import csv
import time
import csv
from django.urls import path
from core_app.models import LicenseNumber
from django.contrib import admin, messages
from django.conf import settings
from django.http import HttpResponseRedirect
from ABC_BizEnrichment.common.helper_function import get_full_function_name, remove_bom, return_response
from core_app.models import *
from django import forms
from django.contrib import admin,messages
from django.conf import settings
from django.http import HttpResponseRedirect
# Logging setup
from core_app.models import LicenseOutput
from collections import defaultdict
from import_export.admin import ExportMixin, ImportExportModelAdmin

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

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
    

def get_or_create_license_number(license_number):
    """Gets or creates a LicenseNumber object based on the provided license number."""
    try:
        license_number = int(license_number)
        license_number_obj, _ = LicenseNumber.objects.get_or_create(license_number=license_number)
        return license_number_obj
    except (ValueError, TypeError):
        return None
def Remove_duplicate_license_numbers():
        # Step 1: Fetch all records from the LicenseOutput model
        records = LicenseOutput.objects.all()
        # Step 2: Identify duplicates based on license_number
        license_number_dict = defaultdict(list)
        for record in records:
            license_number_dict[record.license_number].append(record)

        # Step 3: Separate unique and duplicate records
        unique_records = []
        duplicate_records = []

        # We will store the primary keys of records to be deleted
        delete_ids = []

        for license_number, records_with_same_license in license_number_dict.items():
            if len(records_with_same_license) == 1:
                # If there's only one record for this license_number, it's unique
                unique_records.append(records_with_same_license[0])
            else:
                # If there are multiple records, they are duplicates. Keep the first one and mark the rest for deletion.
                unique_records.append(records_with_same_license[0])
                duplicate_records.extend(records_with_same_license[1:])
                delete_ids.extend([record.id for record in records_with_same_license[1:]])

        # Step 4: Delete the duplicate records from the database
        LicenseOutput.objects.filter(id__in=delete_ids).delete()

        # Step 5: Save the unique records to a CSV file
        with open('unique_records.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Writing header (field names of the model)
            writer.writerow([field.name for field in LicenseOutput._meta.fields])
            
            # Writing records
            for record in unique_records:
                writer.writerow([getattr(record, field.name) for field in LicenseOutput._meta.fields])

        # Step 6: Optionally, save the duplicate records to a separate CSV file for reference
        with open('duplicate_records.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Writing header (field names of the model)
            writer.writerow([field.name for field in LicenseOutput._meta.fields])
            
            # Writing duplicate records
            for record in duplicate_records:
                writer.writerow([getattr(record, field.name) for field in LicenseOutput._meta.fields])

        print("Duplicate records have been deleted, and unique and duplicate records are saved to CSV files.")

class CSVImportForm(forms.Form):
    csv_file = forms.FileField(label="Upload CSV File", required=True)

class BaseCSVImportAdmin(CSVImportAdminMixin,admin.ModelAdmin):
    def process_csv_import(self, request, model_class, field_mappings):
        full_function_name = get_full_function_name()
        
        if request.method != "POST":
            return return_response(request, "admin/csv_form.html", 
                                context={"opts": self.model._meta, 
                                       "form": CSVImportForm(), 
                                       **self.admin_site.each_context(request)})
        
        csv_file = remove_bom(request.FILES["csv_file"].file)
        reader = csv.DictReader(csv_file)
        batch_size = getattr(settings, 'BATCH_SIZE', 1000)
        try:
            rows = list(reader)
            logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {len(rows)}")
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for row in batch:
                    try:
                        data = {k: field_mappings[k](row) for k in field_mappings}
                        # print('field_mappings', field_mappings)
                        model_class.objects.create(**data)
                        logger.info(f"{full_function_name}: Created new record with entity_num {data}.")
                    except Exception as e:
                        logger.error(f"{full_function_name}: Error creating record: {str(e)}")
                logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                time.sleep(10)
            self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
            return HttpResponseRedirect(f"/admin/core_app/{model_class._meta.model_name}/")
        except Exception as e:
            logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
            self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return None
        
