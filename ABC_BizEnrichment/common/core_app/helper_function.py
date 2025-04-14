import csv
import os
from django.db import IntegrityError
from django.urls import path
from django.contrib import admin, messages
from django.http import HttpResponseRedirect
import pandas as pd
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import return_response
from django import forms
from django.contrib import admin,messages
from django.http import HttpResponseRedirect
from django.shortcuts import render, redirect



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
    csv_file = forms.FileField(
        label="Upload CSV/Excel Files",
        required=True,
        # attrs={'multiple': True}
        # multiple=True,
    )

class BaseCSVImportAdmin(CSVImportAdminMixin, admin.ModelAdmin):

    def process_csv_import(self, request, model_class, field_mappings):
        if request.method != "POST":
            return render(
                request,
                "admin/csv_form.html",
                context={
                    "opts": self.model._meta,
                    "form": CSVImportForm(),
                    **self.admin_site.each_context(request),
                },
            )

        # Support multiple files
        uploaded_files = request.FILES.getlist("csv_file")
        if not uploaded_files:
            messages.error(request, "No CSV or Excel files uploaded.")
            return redirect(request.path)

        total_skipped_rows = 0
        total_records_created = 0

        try:
            for file in uploaded_files:
                file_extension = os.path.splitext(file.name)[1].lower()

                # Read the file into a DataFrame
                if file_extension == ".csv":
                    df = pd.read_csv(file, engine="python", sep=r"\*\|\*", encoding="utf-8", on_bad_lines="skip")
                elif file_extension in [".xls", ".xlsx"]:
                    df = pd.read_excel(file)
                else:
                    messages.warning(request, f"Unsupported file type skipped: {file.name}")
                    continue

                rows = df.to_dict(orient="records")
                total_records = len(rows)
                records_to_create = []
                skipped_rows = 0
                for row in tqdm(rows, desc=f"Processing {file.name}", unit="record", total=total_records):
                    try:
                        data = {k: field_mappings[k](row) for k in field_mappings}
                        instance = model_class(**data)
                        records_to_create.append(instance)
                    except Exception as e:
                        skipped_rows += 1
                        print(f"Skipping row due to error: {e}, Row: {row}")
                        continue

                    if len(records_to_create) >= 10000:
                        model_class.objects.bulk_create(records_to_create)
                        total_records_created += len(records_to_create)
                        records_to_create.clear()

                if records_to_create:
                    model_class.objects.bulk_create(records_to_create)
                    total_records_created += len(records_to_create)

                total_skipped_rows += skipped_rows

            messages.success(
                request,
                f"Files imported successfully! {total_records_created} records created. "
                f"Skipped {total_skipped_rows} rows due to errors."
            )
            return HttpResponseRedirect(f"/admin/core_app/{model_class._meta.model_name}")
        except Exception as e:
            messages.error(request, f"Error importing files: {str(e)}")
            return redirect(request.path)

        
