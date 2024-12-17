import csv
from datetime import datetime
import logging
import time
from django import forms
from django.contrib import admin,messages
from django.conf import settings
from django.http import HttpResponseRedirect
from ABC_BizEnrichment.common.core_app.helper_function import CSVImportAdminMixin, get_or_create_license_number
from ABC_BizEnrichment.common.helper_function import get_full_function_name, parse_date, remove_bom, return_response
from core_app.models import CompanyInformation, LicenseNumber, LicenseOutput, YelpRestaurantRecord

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())
class CSVImportForm(forms.Form):
    csv_file = forms.FileField(label="Upload CSV File", required=True)


@admin.register(LicenseNumber)
class LicenseNumberAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ['id','license_number']
    actions = ['licenses_csv'] 
    csv_import_url_name = "licenses" 
    def get_import_view(self):
        def importlicensenumber(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                csv_file = remove_bom(request.FILES["csv_file"].file)
                file_name = request.FILES["csv_file"].name
                logger.info(f"{full_function_name}: Uploaded file name: {file_name}")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            licenseNumber = row['License Numbers']
                            try:
                                LicenseNumber.objects.create(license_number=licenseNumber)
                                logger.info(f"{full_function_name}: Created new record with entity_num {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for entity_num {licenseNumber}: {str(e)}")
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/licensenumber/")  # This redirects to the changelist 
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return return_response(request, "admin/csv_form.html", context = { **self.admin_site.each_context(request),"opts": self.model._meta, "form": CSVImportForm()})
        return importlicensenumber
    

# License Output Import
@admin.register(LicenseOutput)
class LicenseOutputAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("id","license_number","licensee","primary_owner" ,"business_name","business_status")
    csv_import_url_name = "license_output"
    search_fields = ("license_number__license_number",)
    def get_import_view(self):
        def Importlicenseoutput(request):
            full_function_name = get_full_function_name()   
            if request.method == "POST":
                csv_file = remove_bom(request.FILES["csv_file"].file)
                file_name = request.FILES["csv_file"].name
                logger.info(f"{full_function_name}: Uploaded file name: {file_name}")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            licenseNumber = row['license_number']
                            expiration_date  = parse_date(row['expiration_date'])
                            print('expiration_date', expiration_date)
                            # break
                            try:
                                LicenseOutput.objects.create(
                                    license_number=get_or_create_license_number(licenseNumber),
                                    primary_owner = row['primary_owner'],
                                    office_of_application = row['office_of_application'],
                                    business_name = row['business_name'],
                                    business_address = row['business_address'],
                                    county = row['county'],
                                    census_tract = float(row['census_tract']) if row['census_tract'] else None,
                                    licensee = row['licensee'],
                                    license_type = row['license_type'],
                                    license_type_status = row['license_type_status'],
                                    status_date = parse_date(row['status_date']),
                                    original_issue_date = parse_date(row['original_issue_date']),
                                    expiration_date = parse_date(row['expiration_date']),
                                    term = row['term'],
                                    master = row['master'],
                                    duplicate = int(row['duplicate']) if row['duplicate'] else None,
                                    fee_code = row['fee_code'],
                                    transfers = row['transfers'],
                                    conditions = row['conditions'],
                                    operating_restrictions = row['operating_restrictions'],
                                    disciplinary_action = row['disciplinary_action'],
                                    disciplinary_history = row['disciplinary_history'],
                                    holds = row['holds'],
                                    escrows = row['escrows'],
                                    transferred_on = parse_date(row['transferred_on']),
                                    to_license_number = row['to_license_number'],
                                    transferred_on2 = parse_date(row['transferred_on2']),
                                    business_name_alt=row['business_name_alt'] if row['business_name_alt'] else 'N/A',
                                    business_address_alt=row['business_address_alt'] if row['business_address_alt'] else 'N/A',
                                    place_name = row['place_name'],
                                    rating = row['rating'],
                                    phone_number = row['phone_number'],
                                    website = row['website'],
                                    types = row['types'],
                                    business_status = row['business_status'] 
                                )
                                logger.info(f"{full_function_name}: Created new record with entity_num {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for entity_num {licenseNumber}: {str(e)}")
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/licenseoutput/")  # This 
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return return_response(request, "admin/csv_form.html", context = { **self.admin_site.each_context(request),"opts": self.model._meta, "form": CSVImportForm()})
        return Importlicenseoutput
    

# Yelp Restaurant Record
@admin.register(YelpRestaurantRecord)
class YelpRestaurantRecordAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    csv_import_url_name = "yelp_data"
    search_fields = ("file_number__license_number",)
    list_display = ( "license_type", "file_number",  "primary_name",   "dba_name",  "prem_addr_1",  "prem_city",   "prem_state",  "prem_zip",   "yelp_rating" )
    def get_import_view(self):
        def ImportYelpRestaurantData(request):
            full_function_name = get_full_function_name()   
            if request.method == "POST":
                csv_file = remove_bom(request.FILES["csv_file"].file)
                file_name = request.FILES["csv_file"].name
                logger.info(f"{full_function_name}: Uploaded file name: {file_name}")
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            license_type=row["License Type"]
                            filenumber=row["Files Name"]
                            file_number =get_or_create_license_number(filenumber)
                            try:
                                YelpRestaurantRecord.objects.create(
                                    license_type=license_type,
                                    file_number=file_number,
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
                                logger.info(f"{full_function_name}: Created new record with File Number {file_number}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for File Number {file_number}: {str(e)}")
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/yelprestaurantrecord/")  # This 
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return return_response(request, "admin/csv_form.html", context = { **self.admin_site.each_context(request),"opts": self.model._meta, "form": CSVImportForm()})
        return ImportYelpRestaurantData
    

    
# Company Information Import
@admin.register(CompanyInformation)
class CompanyInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("id","license_number", "type", "name", "role")
    csv_import_url_name = "company_information"
    def get_import_view(self):
        def Importcompanyinformation(request):
            full_function_name = get_full_function_name()   
            if request.method == "POST":
                csv_file = remove_bom(request.FILES["csv_file"].file)
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            licenseNumber = row['License Number']
                            try:
                                CompanyInformation.objects.create(     license_number=get_or_create_license_number(licenseNumber), type= row["Type"],  name=row["Name"], role= row["Role"])
                                logger.info(f"{full_function_name}: Created new record with entity_num {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error creating record for entity_num {licenseNumber}: {str(e)}")
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                        time.sleep(10)
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/companyinformation/")  # This 
                except Exception as e:
                    logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            return return_response(request, "admin/csv_form.html", context = { **self.admin_site.each_context(request),"opts": self.model._meta, "form": CSVImportForm()})
        return Importcompanyinformation
    
