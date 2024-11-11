import csv
from django.core.management.base import BaseCommand
from core_app.models import AbcBizYelpRestaurantData, LicenseNumber
from openpyxl import load_workbook
from decimal import Decimal, InvalidOperation
class Command(BaseCommand):
    help = 'Import Yelp restaurant data from an Excel file'

    def handle(self, *args, **options):
        # Use the correct file path
        file_path = r"c:\Users\Exotica\Downloads\regauravvatsandmichaelbrewer\Abc_biz_ Yelp restaurant data - Step 3.xlsx"
        
        # Load the Excel file
        wb = load_workbook(file_path)
        sheet = wb.active  # Or specify the sheet name if necessary

        # Iterate over each row in the sheet (skipping header row)
        for row in sheet.iter_rows(min_row=2, values_only=True):
            (
                license_type, 
                file_number, 
                primary_name, 
                dba_name, 
                prem_addr_1, 
                prem_addr_2, 
                prem_city, 
                prem_state, 
                prem_zip, 
                yelp_link, 
                yelp_name, 
                yelp_phone, 
                yelp_web_site, 
                yelp_rating, 
                *extra
            ) = row[:14]

            if not yelp_name:
                continue

            print("file_number : ", file_number)
            # final_file_number = LicenseNumber.objects.get(license_number=file_number)
            
            try:
                final_file_number = LicenseNumber.objects.get(license_number=file_number)
                print("final_file_number : ", final_file_number)
            except LicenseNumber.DoesNotExist:
                final_file_number = None
                print("final_file_number : ", final_file_number)
            abc_data = AbcBizYelpRestaurantData(
                license_type=license_type,
                file_number=final_file_number,
                primary_name=primary_name,
                dba_name=dba_name,
                prem_addr_1=prem_addr_1,
                prem_addr_2=prem_addr_2,
                prem_city=prem_city,
                prem_state=prem_state,
                prem_zip=prem_zip,
                yelp_link=yelp_link,
                yelp_name=yelp_name,
                yelp_phone=yelp_phone,
                yelp_web_site=yelp_web_site,
                yelp_rating=yelp_rating
            )

            abc_data.save()

        self.stdout.write(self.style.SUCCESS('Data imported successfully'))