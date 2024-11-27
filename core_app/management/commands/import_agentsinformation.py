import csv
from django.core.management.base import BaseCommand
from core_app.models import AgentsInformation

class Command(BaseCommand):
    help = 'Import agents information from a CSV file'

    def handle(self, *args, **kwargs):
        file_path = r"C:\Users\Exotica\Videos\abhishek\regauravvatsandmichaelbrewer\Agents_Information 1 - Agent info(Good to have only) - Step 4.csv"

        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)

            # Print the headers to check the column names
            print("CSV Headers:", csv_reader.fieldnames)

            for row in csv_reader:
                # Print the full row for debugging purposes
                # print("Row data:", row)

                entity_name = row.get('ENTITY_NAME')
                
                # print(f"Processing entity_name: {entity_name}")

                if not entity_name:
                    entity_name = None
                    # print("Skipping row with missing entity_name")
                    # continue

                # Create or update an AgentsInformation record
                agent_info, created = AgentsInformation.objects.update_or_create(
                    entity_name=entity_name,
                    defaults={
                        'entity_num': row.get('ENTITY_NUM'),
                        'org_name': row.get('ORG_NAME'),
                        'first_name': row.get('FIRST_NAME'),
                        'middle_name': row.get('MIDDLE_NAME'),
                        'last_name': row.get('LAST_NAME'),
                        'physical_address1': row.get('PHYSICAL_ADDRESS1'),
                        'physical_address2': row.get('PHYSICAL_ADDRESS2'),
                        'physical_address3': row.get('PHYSICAL_ADDRESS4'),
                        'physical_city': row.get('PHYSICAL_CITY'),
                        'physical_state': row.get('PHYSICAL_STATE'),
                        'physical_country': row.get('PHYSICAL_COUNTRY'),
                        'physical_postal_code': row.get('PHYSICAL_POSTAL_CODE'),
                        'agent_type': row.get('AGENT_TYPE'),
                    }
                )

                if created:
                    self.stdout.write(self.style.SUCCESS(f"Added new record: {agent_info.entity_name}"))
                else:
                    self.stdout.write(self.style.SUCCESS(f"Updated existing record: {agent_info.entity_name or 'Unnamed Record'}"))

        self.stdout.write(self.style.SUCCESS('Data import complete.'))
