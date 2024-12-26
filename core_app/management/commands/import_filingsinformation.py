import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from core_app.models import FilingsInformation  # Import the FilingsInformation model

class Command(BaseCommand):
    help = 'Import license numbers from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\Filings_Information 1 - Master File - SOS output - Step 4.csv"

        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip the header row if present

            for row in csv_reader:
                # Convert date string in "DD-MMM-YY" format to "YYYY-MM-DD" format
                def convert_date(date_str):
                    try:
                        # Assuming the date format in the CSV is "DD-MMM-YY" (e.g., "02-Apr-04")
                        return datetime.strptime(date_str, "%d-%b-%y").date() if date_str else None
                    except ValueError:
                        return None

                # Safe conversion for 'dup_counts' field to handle empty or malformed data
                def safe_int(value):
                    try:
                        # Strip spaces and attempt conversion to int
                        return int(value.strip()) if value.strip() else None
                    except ValueError:
                        return None  # Return None if conversion fails

                # Ensure correct mapping between CSV columns and model fields
                FilingsInformation.objects.create(
                    license_type=row[0],
                    file_number=row[1],
                    lic_or_app=row[2],
                    type_status=row[3],
                    type_orig_iss_date=convert_date(row[4]),  # Convert date
                    expir_date=convert_date(row[5]),  # Convert date
                    fee_codes=row[6] if row[6] else None,
                    dup_counts=safe_int(row[7]),  # Safely convert to integer
                    master_ind=row[8],
                    term_in_number_of_months=int(row[9]) if row[9] else None,
                    geo_code=row[10] if row[10] else None,
                    district=row[11] if row[11] else None,
                    primary_name=row[12] if row[12] else None,
                    prem_addr_1=row[13] if row[13] else None,
                    prem_addr_2=row[14] if row[14] else None,
                    prem_city=row[15] if row[15] else None,
                    prem_state=row[16] if row[16] else None,
                    prem_zip=row[17] if row[17] else None,
                    dba_name=row[18] if row[18] else None,
                    mail_addr_1=row[19] if row[19] else None,
                    mail_addr_2=row[20] if row[20] else None,
                    mail_city=row[21] if row[21] else None,
                    mail_state=row[22] if row[22] else None,
                    mail_zip=row[23] if row[23] else None,
                    prem_county=row[24] if row[24] else None,
                    prem_census_tract=row[25] if row[25] else None,
                    entity_num=row[26] if row[26] else None,
                    initial_filing_date=convert_date(row[27]),  # Convert date
                    jurisdiction=row[28] if row[28] else None,
                    entity_status=row[29],
                    standing_sos=row[30] if row[30] else None,
                    entity_type=row[31] if row[31] else None,
                    filing_type=row[32] if row[32] else None,
                    foreign_name=row[33] if row[33] else None,
                    standing_ftb=row[34] if row[34] else None,
                    standing_vcfcf=row[35] if row[35] else None,
                    standing_agent=row[36] if row[36] else None,
                    suspension_date=convert_date(row[37]),  # Convert date
                    last_si_file_number=row[38] if row[38] else None,
                    last_si_file_date=convert_date(row[39]),  # Convert date
                    llc_management_structure=row[40] if row[40] else None,
                    type_of_business=row[41] if row[41] else None
                )
            self.stdout.write(self.style.SUCCESS('Data successfully imported into FilingsInformation'))
