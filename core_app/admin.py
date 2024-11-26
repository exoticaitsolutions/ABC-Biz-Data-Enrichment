import logging
import time
import csv
from django import forms
from django.contrib import admin
from ABC_BizEnrichment.common.core_app.helper_function import CSVImportAdminMixin
from ABC_BizEnrichment.common.helper_function import get_full_function_name
from .models import *
from django.contrib import messages
from .forms import CSVImportForm
from io import TextIOWrapper
from django.urls import path
from django.template.response import TemplateResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())
 
 
class CSVImportForm(forms.Form):
    csv_file = forms.FileField(label="Upload CSV File", required=True)

@admin.register(LicenseNumber)
class LicenseNumberAdmin(admin.ModelAdmin):
    list_display = ['license_number']
    actions = ['licenses_csv']  # Make sure the action is added

    def licenses_csv(self, request):
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                # csv_file = TextIOWrapper(request.FILES["csv_file"].file)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")

                reader = csv.DictReader(csv_file)
                try:
                    for row in reader:
                        print(row)
                        # license_number = int(row[0].strip())  # Ensure the data is cleaned and in integer format
                        LicenseNumber.objects.create(license_number=row["License Numbers"])
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV---: {str(e)}", messages.ERROR)

                # self.stdout.write(self.style.SUCCESS('Successfully imported license numbers'))
       
        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    licenses_csv.short_description = "Import CSV"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("licenses-csv/", self.licenses_csv, name="licenses_csv"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['licenses_csv_url'] = 'admin:licenses_csv'
        return super().changelist_view(request, extra_context=extra_context)



def get_or_create_license_number(license_number):
    """Gets or creates a LicenseNumber object based on the provided license number."""
    try:
        license_number = int(license_number)
        license_number_obj, _ = LicenseNumber.objects.get_or_create(license_number=license_number)
        return license_number_obj
    except (ValueError, TypeError):
        return None
    
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
    
def safe_parse_date(date_str):
    """
    Safely parse a date string into YYYY-MM-DD format.
    Returns None if the date is invalid or empty.
    """
    if not date_str or date_str.strip() == "" or date_str == "“”":
        return None  # Return None for empty, invalid, or specific "invalid" date string

    # Remove any unwanted characters (e.g., smart quotes)
    date_str = date_str.strip().replace("“", "").replace("”", "")

    # Try parsing the date in multiple formats
    date_formats = ["%d-%b-%y", "%Y-%m-%d", "%m/%d/%Y"]  # Accepted date formats
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue  # Try the next format

    return None  # Return None if no formats match
@admin.register(CompanyInformation)
class CompanyInformationAdmin(admin.ModelAdmin):
    list_display = ("license_number", "type", "name", "role")

    def import_csv(self, request):
        """
        This action will allow the admin to upload a CSV to import data.
        """
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
                print("csv_file : ", csv_file)
                reader = csv.DictReader(csv_file)
                print("reader : ", reader)
                try:
                    for row in reader:
                        
                        # license_number = LicenseNumber.objects.get(license_number=int(row['License Number']))
                        # Update or create rows from the CSV data
                        CompanyInformation.objects.create(
                            license_number=get_or_create_license_number(row['License Number']),
                            type= row["Type"],
                            name=row["Name"],
                            role= row["Role"],
                           
                        )
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV---: {str(e)}", messages.ERROR)
            else:
                self.message_user(request, "Invalid CSV file!", messages.ERROR)
        
        # Render the form for uploading CSV
        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    import_csv.short_description = "Import CSV"

    def get_urls(self):
        """
        Overriding the default get_urls to add a URL for the CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("import-csv/", self.import_csv, name="import_csv"),
        ]
        return custom_urls + urls
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        # Use the correct URL name 'admin:import_csv'
        extra_context['import_csv_url'] = 'admin:import_csv'
        return super().changelist_view(request, extra_context=extra_context)

@admin.register(LicenseOutput)
class LicenseOutputAdmin(admin.ModelAdmin):
    list_display = ("license_number", "business_name", "business_status","licensee")  # Adjust to your needs

    def output_csv(self, request):
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
                reader = csv.DictReader(csv_file)
                try:
                    for row in reader:
                        print(row)
                        LicenseOutput.objects.create(
                            license_number=get_or_create_license_number(row['License Number']),
                            primary_owner = row['Primary Owner'],
                            office_of_application = row['Office of Application'],
                            business_name = row['Business Name'],
                            business_address = row['Business Address'],
                            county = row['County'],
                            census_tract = float(row['Census Tract']) if row['Census Tract'] else None,
                            licensee = row['Licensee'],
                            license_type = row['LICENSE TYPE'],
                            license_type_status = row['License Type Status'],
                            
                            status_date = convert_to_date(row['Status Date']),
                            original_issue_date = convert_to_date(row['Original Issue Date']) if row['Original Issue Date'] else None,
                            expiration_date = convert_to_date(row['Expiration Date']) if row['Expiration Date'] else None,
                            
                            term = row['Term'],
                            master = row['Master'],
                            duplicate = int(row['Duplicate']) if row['Duplicate'] else None,
                            fee_code = row['Fee Code'],
                            transfers = row['Transfers'],
                            conditions = row['Conditions'],
                            operating_restrictions = row['Operating Restrictions'],
                            disciplinary_action = row['Disciplinary Action'],
                            disciplinary_history = row['Disciplinary History'],
                            holds = row['Holds'],
                            escrows = row['Escrows'],
                            from_license_number = row['From License Number'],
                            transferred_on = convert_to_date(row['Transferred On']),
                            to_license_number = row['To License Number'],
                            transferred_on2 = convert_to_date(row['Transferred On2']),
                            business_name_alt = row['Business Name'],
                            business_address_alt = row['Business Address'],
                            place_name = row[' Place Name'],
                            rating = row[' Rating'],
                            phone_number = row[' Phone Number'],
                            website = row[' Website'],
                            types = row[' Types'],
                            business_status = row[' Business Status'],
                        )

                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            else:
                self.message_user(request, "Invalid CSV file!", messages.ERROR)
        
        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    output_csv.short_description = "Import CSV"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("output-csv", self.output_csv, name="output_csv"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        # Use the correct URL name 'admin:import_csv'
        extra_context['output_csv_url'] = 'admin:output_csv'
        return super().changelist_view(request, extra_context=extra_context)
# # admin.site.register(LicenseOutput, LicenseOutputAdmin)
@admin.register(AbcBizYelpRestaurantData)
class AbcBizYelpRestaurantDataAdmin(admin.ModelAdmin):
    list_display = (
        "license_type", 
        "file_number", 
        "primary_name", 
        "dba_name", 
        "prem_addr_1", 
        "prem_city", 
        "prem_state", 
        "prem_zip", 
        "yelp_rating"
    )
    search_fields = ("primary_name", "dba_name", "prem_city", "prem_state", "prem_zip")
    list_filter = ("prem_state", "license_type")

    def import_csv(self, request):
        """
        Custom action to upload and import data from a CSV file.
        """
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                csv_file = TextIOWrapper(request.FILES["csv_file"].file,encoding="utf-8", errors="replace")
                reader = csv.DictReader(csv_file)

                print(reader)
                try:
                    for row in reader:
                        # Assuming 'file_number' maps to 'LicenseNumber'
                        print(row)
                        files_name = row["Files Name"]
                        print(files_name,"files_namefiles_namefiles_namefiles_name")
                        AbcBizYelpRestaurantData.objects.create(
                            license_type=row["License Type"],
                            file_number=get_or_create_license_number(files_name),
                            primary_name=row.get("Primary Name", "").strip(),
                            dba_name=row["DBA Name"],
                            prem_addr_1=row["Prem Addr 1"],
                            prem_addr_2=row.get("Prem Addr 2", "").strip(),
                            prem_city=row["Prem City"],
                            prem_state=row["Prem State"],
                            prem_zip=row["Prem Zip"],
                            yelp_link=row.get("Yelp Link", "").strip(),
                            yelp_name=row.get("Yelp Name", "").strip(),
                            yelp_phone=row.get("Yelp Phone", "").strip(),
                            yelp_web_site=row.get("Yelp Web Site", "").strip(),
                            yelp_rating=row.get("Yelp Rating", "").strip(),
                        )
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            else:
                self.message_user(request, "Invalid CSV file!", messages.ERROR)

        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    import_csv.short_description = "Import CSV"

    def get_urls(self):
        """
        Add a custom URL for CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("resturant-csv/", self.import_csv, name="resturant_csv"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['resturant_csv_url'] = 'admin:resturant_csv'
        return super().changelist_view(request, extra_context=extra_context)
def checked_the_csv_headers(csv_headers , default_headers):
    if csv_headers == default_headers:
        return True
    else:
        return False

def get_or_create_entity_number(entity_number):
    """Gets or creates a LicenseNumber object based on the provided license number."""
    try:
        entity_number = int(entity_number)
        license_number_obj, _ = LicenseeNameDataEnrichment.objects.get_or_create(license_number=entity_number)
        return license_number_obj
    except (ValueError, TypeError):
        return None
@admin.register(AgentsInformation)
class AgentsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("entity_name", "first_name", "last_name", "physical_city", "agent_type")
    csv_import_url_name = "agentsinformation" 
    def get_import_view(self):
        def import_csv(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8", errors="replace")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = 3000  # Number of rows to import at a time
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        
                        for row in batch:
                            entity_num = row['ENTITY_NUM']
                            try:
                                # Always create a new record
                                AgentsInformation.objects.create(
                                    entity_num=entity_num,
                                    entity_name=row['ENTITY_NAME'],
                                    org_name=row['ORG_NAME'],
                                    first_name=row['FIRST_NAME'],
                                    middle_name=row['MIDDLE_NAME'],
                                    last_name=row['LAST_NAME'],
                                    physical_address1=row['PHYSICAL_ADDRESS1'],
                                    physical_address2=row['PHYSICAL_ADDRESS2'],
                                    physical_address3=row['PHYSICAL_ADDRESS3'],
                                    physical_city=row['PHYSICAL_CITY'],
                                    physical_state=row['PHYSICAL_STATE'],
                                    physical_country=row['PHYSICAL_COUNTRY'],
                                    physical_postal_code=row['PHYSICAL_POSTAL_CODE'],
                                    agent_type=row['AGENT_TYPE'],
                                )
                                logger.info(f"{full_function_name}: Created new record with entity_num {entity_num}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for entity_num {entity_num}: {str(e)}")
                        
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)  # Wait 10 seconds before processing the next batch
                    
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)

            form = CSVImportForm()
            context = {
                **self.admin_site.each_context(request),
                "opts": self.model._meta,
                "form": form,
            }
            return TemplateResponse(request, "admin/csv_form.html", context)
        return import_csv
# FilingsInformationAdmin Start 
@admin.register(FilingsInformation)
class FilingsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("file_number", "license_type", "type_status", "primary_name", "jurisdiction")
    csv_import_url_name = "filingsinformation"
    def get_import_view(self):
        def import_csv(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8", errors="replace")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = 10000  # Number of rows to import at a time
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            file_number = row['File_Number']
                            try:
                                # Always create a new record
                                FilingsInformation.objects.create(
                                file_number=file_number,  # The unique identifier
                                license_type=row['License_Type'],
                                lic_or_app=row['Lic_or_App'],
                                type_status=row['Type_Status'],
                                type_orig_iss_date=safe_parse_date(row['Type_Orig_Iss_Date']),
                                expir_date=safe_parse_date(row['Expir_Date']),
                                fee_codes=row['Fee_Codes'],
                                master_ind=row['Master_Ind'],
                                term_in_number_of_months=row['Term_in_#_of_Months'],
                                geo_code=row['Geo_Code'],
                                district=row['District'],
                                primary_name=row['Primary_Name'],
                                prem_addr_1=row['Prem_Addr_1'],
                                prem_addr_2=row['Prem_Addr_2'],
                                prem_city=row['Prem_City'],
                                prem_state=row['Prem_State'],
                                prem_zip=row['Prem_Zip'],
                                dba_name=row['DBA_Name'],
                                mail_addr_1=row['Mail_Addr_1'],
                                mail_addr_2=row['Mail_Addr_2'],
                                mail_city=row['Mail_City'],
                                mail_state=row['Mail_State'],
                                mail_zip=row['Mail_Zip'],
                                prem_county=row['Prem_County'],
                                prem_census_tract=row['Prem_Census_Tract_#'],
                                initial_filing_date=safe_parse_date(row['INITIAL_FILING_DATE']),
                                jurisdiction=row['JURISDICTION'],
                                entity_status=row['ENTITY_STATUS'],
                                standing_sos=row['STANDING_SOS'],
                                entity_type=row['ENTITY_TYPE'],
                                filing_type=row['FILING_TYPE'],
                                foreign_name=row['FOREIGN_NAME'],
                                standing_ftb=row['STANDING_FTB'],
                                standing_vcfcf=row['STANDING_VCFCF'],
                                standing_agent=row['STANDING_AGENT'],
                                entity_num=row['ENTITY_NUM'],
                                suspension_date=safe_parse_date(row['SUSPENSION_DATE']),
                                last_si_file_number=row['LAST_SI_FILE_NUMBER'],
                                last_si_file_date=safe_parse_date(row['LAST_SI_FILE_DATE']),
                                llc_management_structure=row['LLC_MANAGEMENT_STRUCTURE'],
                                type_of_business=row['TYPE_OF_BUSINESS'],
                            )
                                logger.info(f"{full_function_name}: Created new record with file_number {file_number}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for file_number {file_number}: {str(e)}")
                        
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)  # Wait 10 seconds before processing the next batch
                    
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            form = CSVImportForm()
            context = {
                **self.admin_site.each_context(request),
                "opts": self.model._meta,
                "form": form,
            }
            return TemplateResponse(request, "admin/csv_form.html", context)
        return import_csv
@admin.register(PrincipalsInformation)
class PrincipalsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("first_name", "last_name", "entity_name", "city", "state")
    csv_import_url_name = "principalsinformation" 
    def get_import_view(self):
        def import_csv(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8", errors="replace")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = 10000  # Number of rows to import at a time
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            entity_num = row['ENTITY_NUM']
                            try:
                                PrincipalsInformation.objects.create(
                                entity_name = row['ENTITY_NAME'],
                                entity_num = entity_num,
                                org_name = row['ORG_NAME'],
                                first_name = row['FIRST_NAME'],
                                middle_name = row['MIDDLE_NAME'],
                                last_name = row['LAST_NAME'],
                                address1 = row['ADDRESS1'],
                                address2 = row['ADDRESS2'],
                                address3 = row['ADDRESS3'],
                                city = row['CITY'],
                                state = row['STATE'],
                                country = row['COUNTRY'],
                                postal_code = row['POSTAL_CODE'],
                                position_1 = row['POSITION1'],
                                position_2 = row['POSITION2'],
                                position_3 = row['POSITION3'],
                                position_4 = row['POSITION4'],
                                position_5 = row['POSITION5'],
                                position_6 = row['POSITION6'],
                                position_7 = row['POSITION7'])
                                logger.info(f"{full_function_name}: Created new record with entity_num {entity_num}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for entity_num {entity_num}: {str(e)}")
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)  # Wait 10 seconds before processing the next batch
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            form = CSVImportForm()
            context = {
                **self.admin_site.each_context(request),
                "opts": self.model._meta,
                "form": form,
            }
            return TemplateResponse(request, "admin/csv_form.html", context)
        return import_csv
@admin.register(LicenseeNameDataEnrichment)
class LicenseeNameDataEnrichmentAdmin(admin.ModelAdmin):
    list_display = ("license_number", "name", "role","licensee", "last_name", "first_name")

    def import_csv(self, request):
        """
        Admin action to upload a CSV and import data into the LicenseeNameDataEnrichment model.
        """
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
                reader = csv.DictReader(csv_file)
                try:
                    for row in reader:
                        LicenseeNameDataEnrichment.objects.create(
                            license_number=get_or_create_license_number(row['License Number']),
                            master= row['Master'],
                            name= row['Name'],
                            entity_num= '',
                            is_entity_num= True,
                            role= row['Role'],
                            licensee= row['LICENSEE'],
                            business_name= row['Business Name'],
                            website= row['Website'],
                            phone_number= row['Phone Number'],
                            last_name= row['Last Name'],
                            first_name= row['First Name'],
                            middle_name= row['Middle Name'],
                            second_middle= row['2 Middle'],
                            suffix= row['Suffix'],
                            full_name= row['Full Name'],
                            validated_work_email= row['Validated Work Email'],
                        )
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            else:
                self.message_user(request, "Invalid CSV file!", messages.ERROR)

        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    import_csv.short_description = "Import CSV"

    def get_urls(self):
        """
        Add a custom URL for the CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("import-csv/", self.import_csv, name="licenseenamedataenrichment_import_csv"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["import_csv_url"] = "admin:licenseenamedataenrichment_import_csv"
        return super().changelist_view(request, extra_context=extra_context)


    