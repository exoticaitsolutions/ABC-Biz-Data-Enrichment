import os
import csv
from django.core.management.base import BaseCommand
from django.apps import apps

class Command(BaseCommand):
    help = 'Export all data from the SQLite database to CSV files for the merge_data app'

    def handle(self, *args, **kwargs):
        # Directory to save the CSV files
        output_dir = 'csv_exports'
        os.makedirs(output_dir, exist_ok=True)

        # Specify the app you want to export data from
        app_label = 'merge_data'

        # Loop through all models in the specified app
        for model in apps.get_app_config(app_label).get_models():
            model_name = model._meta.model_name
            file_name = f"{output_dir}/{app_label}_{model_name}.csv"

            # Open a CSV file for the model
            with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)

                # Get field names
                field_names = [field.name for field in model._meta.fields]
                writer.writerow(field_names)  # Write header row

                # Write data rows
                for obj in model.objects.all():
                    writer.writerow([getattr(obj, field) for field in field_names])

            self.stdout.write(f"Exported {app_label}.{model_name} to {file_name}")

        self.stdout.write(f"All data from '{app_label}' exported successfully to the '{output_dir}' directory!")
