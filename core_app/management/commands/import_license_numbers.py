import csv
from django.core.management.base import BaseCommand

from core_app.models import LicenseNumber

class Command(BaseCommand):
    help = 'Import license numbers from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\License Numbers Sample - Input for Scraper - Input for Start #1.csv"

        with open(file_path, mode='r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip the header row if present

            for row in csv_reader:
                license_number = int(row[0].strip())  # Ensure the data is cleaned and in integer format
                LicenseNumber.objects.create(license_number=license_number)

        self.stdout.write(self.style.SUCCESS('Successfully imported license numbers'))
