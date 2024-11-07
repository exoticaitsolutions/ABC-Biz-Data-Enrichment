import csv
from django.core.management.base import BaseCommand
from core_app.models import CompanyInformation

class Command(BaseCommand):
    help = 'Import Company Information from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = 'C:/Users/home/Videos/Files/regauravvatsandmichaelbrewer/regauravvatsandmichaelbrewer/Company Information from ABC - Officer name and Title_Role - Step 2.csv'

        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                license_number = int(row['License Number'])  # Convert to integer if the data type is integer
                type = row['Type']
                name = row['Name']
                role = row['Role']

                # Create and save CompanyInformation instance
                CompanyInformation.objects.create(
                    license_number=license_number,
                    type=type,
                    name=name,
                    role=role
                )

        self.stdout.write(self.style.SUCCESS('Company Information data imported successfully.'))
