import csv
from django.core.management.base import BaseCommand
from core_app.models import LicenseeNameDataEnrichment
from openpyxl import load_workbook

class Command(BaseCommand):
    help = 'Import Yelp restaurant data from an Excel file'

    def handle(self, *args, **options):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\licensee name data enrcihment.xlsx"
        
        wb = load_workbook(file_path)
        sheet = wb.active# Skip the header row if present
        for row in sheet.iter_rows(min_row=2, values_only=True):  # Skip header row
            # Handle the data similarly as before
            LicenseeNameDataEnrichment.objects.create(
                license_number=row[0],
                master=row[1],
                name=row[2],
                role=row[3],
                licensee=row[4],
                business_name=row[5],
                website=row[6],
                phone_number=row[7],
                last_name=row[8],
                first_name=row[9],
                middle_name=row[10],
                second_middle=row[11],
                suffix=row[12],
                full_name=row[13],
                validated_work_email=row[14]
            )

        self.stdout.write(self.style.SUCCESS('Data successfully imported into LicenseeNameDataEnrichment'))

        #         for row in csv_reader:
        #             # Utility function to handle empty values
        #             def safe_str(value):
        #                 return value.strip() if value else None

        #             def safe_email(value):
        #                 return value.strip() if value and '@' in value else None

        #             # Create LicenseeNameDataEnrichment record
        #             LicenseeNameDataEnrichment.objects.create(
        #                 license_number=safe_str(row[0]),
        #                 master=safe_str(row[1]),
        #                 name=safe_str(row[2]),
        #                 role=safe_str(row[3]),
        #                 licensee=safe_str(row[4]),
        #                 business_name=safe_str(row[5]),
        #                 website=safe_str(row[6]),
        #                 phone_number=safe_str(row[7]),
        #                 last_name=safe_str(row[8]),
        #                 first_name=safe_str(row[9]),
        #                 middle_name=safe_str(row[10]),
        #                 second_middle=safe_str(row[11]),
        #                 suffix=safe_str(row[12]),
        #                 full_name=safe_str(row[13]),
        #                 validated_work_email=safe_email(row[14])
        #             )

        #     self.stdout.write(self.style.SUCCESS('Data successfully imported into LicenseeNameDataEnrichment'))

        # except UnicodeDecodeError as e:
        #     self.stdout.write(self.style.ERROR(f"UnicodeDecodeError: {str(e)}"))
        #     self.stdout.write(self.style.ERROR("Try using a different encoding, such as ISO-8859-1 or Windows-1252."))

