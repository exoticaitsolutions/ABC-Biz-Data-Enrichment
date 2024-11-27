import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from core_app.models import PrincipalsInformation  # Import the PrincipalsInformation model

class Command(BaseCommand):
    help = 'Import principals information from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\Principals_Information 1 - Officer Name and Title_Role - Step 4.csv"

        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip the header row if present

            for row in csv_reader:
                # Utility function to handle empty values and date conversion
                def convert_date(date_str):
                    try:
                        return datetime.strptime(date_str, "%d-%b-%y").date() if date_str else None
                    except ValueError:
                        return None

                def safe_str(value):
                    return value.strip() if value else None

                # Create PrincipalsInformation record
                PrincipalsInformation.objects.create(
                    entity_name=safe_str(row[0]),
                    entity_num=safe_str(row[1]),
                    org_name=safe_str(row[2]),
                    first_name=safe_str(row[3]),
                    middle_name=safe_str(row[4]),
                    last_name=safe_str(row[5]),
                    position_type=safe_str(row[6]),
                    address1=safe_str(row[7]),
                    address2=safe_str(row[8]),
                    address3=safe_str(row[9]),
                    city=safe_str(row[10]),
                    state=safe_str(row[11]),
                    country=safe_str(row[12]),
                    postal_code=safe_str(row[13]),
                    principal_address=safe_str(row[14]),
                    principal_address2=safe_str(row[15]),
                    principal_city=safe_str(row[16]),
                    principal_state=safe_str(row[17]),
                    principal_country=safe_str(row[18]),
                    principal_postal_code=safe_str(row[19]),
                    principal_address_in_ca=safe_str(row[20]),
                    principal_address2_in_ca=safe_str(row[21]),
                    principal_city_in_ca=safe_str(row[22]),
                    principal_state_in_ca=safe_str(row[23]),
                    principal_country_in_ca=safe_str(row[24]),
                    principal_postal_code_in_ca=safe_str(row[25])
                )

            self.stdout.write(self.style.SUCCESS('Data successfully imported into PrincipalsInformation'))
