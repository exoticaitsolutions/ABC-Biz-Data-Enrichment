import openpyxl
from django.core.management.base import BaseCommand
from core_app.models import LicenseeNameDataEnrichment, LicenseNumber

class Command(BaseCommand):
    help = 'Import licensee name data enrichment from an Excel file'

    def handle(self, *args, **kwargs):
        # Specify the Excel file path
        file_path = r'C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\licensee name data enrcihment.xlsx'
        
        try:
            # Load the Excel workbook
            workbook = openpyxl.load_workbook(file_path)
            sheet = workbook.active  # Get the active worksheet

            # Iterate through rows, assuming the first row is the header
            headers = [cell.value for cell in sheet[1]]  # Read the first row as headers
            
            for row in sheet.iter_rows(min_row=2, values_only=True):
                data = dict(zip(headers, row))  # Map headers to row values

                # Handle ForeignKey: Get or create LicenseNumber object
                license_number = None
                if data.get('license_number'):
                    license_number, _ = LicenseNumber.objects.get_or_create(
                        license_number=data['License Number']
                    )
                
                # Create or update LicenseeNameDataEnrichment object
                enrichment, created = LicenseeNameDataEnrichment.objects.update_or_create(
                    license_number=license_number,
                    defaults={
                        'master': data.get('Master', ''),
                        'name': data.get('Name', ''),
                        'role': data.get('Role', ''),
                        'licensee': data.get('LICENSEE', ''),
                        'business_name': data.get('Business Name', ''),
                        'website': data.get('Website', ''),
                        'phone_number': data.get('Phone Number', ''),
                        'last_name': data.get('Last Name', ''),
                        'first_name': data.get('First Name', ''),
                        'middle_name': data.get('Middle Name', ''),
                        'second_middle': data.get('2 Middle', ''),
                        'suffix': data.get('Suffix', ''),
                        'full_name': data.get('Full Name', ''),
                        'validated_work_email': data.get('Validated Work Email', ''),
                    }
                )
                
                if created:
                    self.stdout.write(self.style.SUCCESS(f"Created: {enrichment}"))
                else:
                    self.stdout.write(self.style.SUCCESS(f"Updated: {enrichment}"))

        except FileNotFoundError:
            self.stderr.write(self.style.ERROR(f"File not found: {file_path}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error: {str(e)}"))

        
        
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

