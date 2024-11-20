from datetime import datetime
from django.core.management.base import BaseCommand
from core_app.models import LicenseOutput,LicenseNumber  # Adjust if necessary
import csv


def convert_to_date(date_string):
    if date_string in ["N/A", "Not Available", "None", "", "null"]:
        return None  # Return None if the date is invalid
    try:
        # Try to parse the date in the format "22-SEP-2003" (DD-MMM-YYYY)
        date_obj = datetime.strptime(date_string, "%d-%b-%Y").date()
        return date_obj
    except ValueError:
        # If the date cannot be parsed, return None
        return None



class Command(BaseCommand):
    help = 'Import License Output data from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\Output License File from ABC and from Google - Step2.csv"  # Update with the correct file path

        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    license_number = LicenseNumber.objects.get(license_number=int(row['License Number']))
                    license_number = license_number
                    primary_owner = row['Primary Owner']
                    office_of_application = row['Office of Application']
                    business_name = row['Business Name']
                    business_address = row['Business Address']
                    county = row['County']
                    census_tract = float(row['Census Tract']) if row['Census Tract'] else None
                    licensee = row['Licensee']
                    license_type = row['LICENSE TYPE']
                    license_type_status = row['License Type Status']
                    
                    # Convert date fields
                    status_date = convert_to_date(row['Status Date'])
                    original_issue_date = convert_to_date(row['Original Issue Date']) if row['Original Issue Date'] else None
                    expiration_date = convert_to_date(row['Expiration Date']) if row['Expiration Date'] else None
                    
                    term = row['Term']
                    master = row['Master']
                    duplicate = int(row['Duplicate']) if row['Duplicate'] else None
                    fee_code = row['Fee Code']
                    transfers = row['Transfers']
                    conditions = row['Conditions']
                    operating_restrictions = row['Operating Restrictions']
                    disciplinary_action = row['Disciplinary Action']
                    disciplinary_history = row['Disciplinary History']
                    holds = row['Holds']
                    escrows = row['Escrows']
                    from_license_number = row['From License Number']
                    transferred_on = convert_to_date(row['Transferred On'])
                    to_license_number = row['To License Number']
                    transferred_on2 = convert_to_date(row['Transferred On2'])
                    business_name_alt = row['Business Name']
                    business_address_alt = row['Business Address']
                    place_name = row[' Place Name']
                    rating = row[' Rating']
                    phone_number = row[' Phone Number']
                    website = row[' Website']
                    types = row[' Types']
                    business_status = row[' Business Status']

                    # Create and save LicenseOutput instance
                    LicenseOutput.objects.create(
                        license_number=license_number,
                        primary_owner=primary_owner,
                        office_of_application=office_of_application,
                        business_name=business_name,
                        business_address=business_address,
                        county=county,
                        census_tract=census_tract,
                        licensee=licensee,
                        license_type=license_type,
                        license_type_status=license_type_status,
                        status_date=status_date,
                        term=term,
                        original_issue_date=original_issue_date,
                        expiration_date=expiration_date,
                        master=master,
                        duplicate=duplicate,
                        fee_code=fee_code,
                        transfers=transfers,
                        conditions=conditions,
                        operating_restrictions=operating_restrictions,
                        disciplinary_action=disciplinary_action,
                        disciplinary_history=disciplinary_history,
                        holds=holds,
                        escrows=escrows,
                        from_license_number=from_license_number,
                        transferred_on=transferred_on,  # Correct format for the model
                        to_license_number=to_license_number,
                        transferred_on2=transferred_on2,
                        business_name_alt=business_name_alt,
                        business_address_alt=business_address_alt,
                        place_name=place_name,
                        rating=rating,
                        phone_number=phone_number,
                        website=website,
                        types=types,
                        business_status=business_status,
                    )

                except KeyError as e:
                    self.stdout.write(self.style.ERROR(f"Missing column in CSV: {e}"))
                    continue
                except ValueError as e:
                    self.stdout.write(self.style.ERROR(f"Invalid value in row: {row}"))
                    continue

        self.stdout.write(self.style.SUCCESS('License Output data imported successfully.'))
