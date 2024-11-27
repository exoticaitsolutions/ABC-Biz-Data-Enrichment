import logging
import time
import csv
from django.conf import settings
from django import forms
from django.contrib import admin
from django.http import HttpResponseRedirect  # Import statement for HttpResponseRedirect
from ABC_BizEnrichment.common.core_app.helper_function import CSVImportAdminMixin, get_or_create_license_number
from ABC_BizEnrichment.common.helper_function import get_full_function_name, safe_parse_date

from django.contrib import messages
from core_app.models import *
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
class LicenseNumberAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ['license_number']
    actions = ['licenses_csv']  # Make sure the action is added
    csv_import_url_name = "licenses" 
    def get_import_view(self):
        def importlicensenumber(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
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
            form = CSVImportForm()
            context = {
                **self.admin_site.each_context(request),
                "opts": self.model._meta,
                "form": form,
            }
            return TemplateResponse(request, "admin/csv_form.html", context)
        return importlicensenumber


@admin.register(CompanyInformation)
class CompanyInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("license_number", "type", "name", "role")
    csv_import_url_name = "company_information"
    def get_import_view(self):
        def Importcompanyinformation(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
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
                    return HttpResponseRedirect("/admin/core_app/companyinformation/")  # This redirects to the changelist view.
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
        return Importcompanyinformation

@admin.register(LicenseOutput)
class LicenseOutputAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("license_number", "business_name", "business_status","licensee")  # Adjust to your needs
    csv_import_url_name = "license_output"
    def get_import_view(self):
        def Importlicenseoutput(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
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
                                LicenseOutput.objects.create(
                                    license_number=get_or_create_license_number(licenseNumber),
                                    primary_owner = row['Primary Owner'],
                                    office_of_application = row['Office of Application'],
                                    business_name = row['Business Name'],
                                    business_address = row['Business Address'],
                                    county = row['County'],
                                    census_tract = float(row['Census Tract']) if row['Census Tract'] else None,
                                    licensee = row['Licensee'],
                                    license_type = row['LICENSE TYPE'],
                                    license_type_status = row['License Type Status'],
                                    status_date = safe_parse_date(row['Status Date']),
                                    original_issue_date = safe_parse_date(row['Original Issue Date']) if row['Original Issue Date'] else None,
                                    expiration_date = safe_parse_date(row['Expiration Date']) if row['Expiration Date'] else None,
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
                                    transferred_on = safe_parse_date(row['Transferred On']),
                                    to_license_number = row['To License Number'],
                                    transferred_on2 = safe_parse_date(row['Transferred On2']),
                                    business_name_alt = row['Business Name'],
                                    business_address_alt = row['Business Address'],
                                    place_name = row[' Place Name'],
                                    rating = row[' Rating'],
                                    phone_number = row[' Phone Number'],
                                    website = row[' Website'],
                                    types = row[' Types'],
                                    business_status = row[' Business Status'] 
                                )
                                logger.info(f"{full_function_name}: Created new record with entity_num {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                    logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/licenseoutput/")  # This redirects to the changelist view.
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
        return Importlicenseoutput

@admin.register(AbcBizYelpRestaurantData)
class AbcBizYelpRestaurantDataAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ( "license_type", "file_number",  "primary_name",   "dba_name",  "prem_addr_1",  "prem_city",   "prem_state",  "prem_zip",   "yelp_rating" )
    search_fields = ("primary_name", "dba_name", "prem_city", "prem_state", "prem_zip")
    # list_filter = ("prem_state", "license_type")
    csv_import_url_name = "yelp_data"
    def get_import_view(self):
        def ImportAbcBizYelpRestaurantData(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            licenseNumber = row['Files Name']
                            try:
                                AbcBizYelpRestaurantData.objects.create(
                                    license_type=row["License Type"],
                                    file_number=get_or_create_license_number(licenseNumber),
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
                                logger.info(f"{full_function_name}: Created new record with licenseNumber {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/abcbizyelprestaurantdata/")  # This redirects to the changelist view.
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
        return ImportAbcBizYelpRestaurantData

@admin.register(AgentsInformation)
class AgentsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("entity_name", "first_name", "last_name", "physical_city", "agent_type")
    csv_import_url_name = "agentsinformation" 
    def get_import_view(self):
        def importagentsinformation(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            entity_num = row['ENTITY_NUM']
                            try:
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
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/agentsinformation/")  # This redirects to the changelist view.
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
        return importagentsinformation


@admin.register(FilingsInformation)
class FilingsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("file_number", "license_type", "type_status", "primary_name", "jurisdiction")
    csv_import_url_name = "filingsinformation" 
    def get_import_view(self):
        def importfilingsinformation(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
                    for i in range(0, total_rows, batch_size):
                        batch = rows[i:i + batch_size]  # Get the current batch
                        for row in batch:
                            file_number = row['File_Number']
                            try:
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
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/filingsinformation/")  # This redirects to the changelist 
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
        return importfilingsinformation



@admin.register(PrincipalsInformation)
class FilingsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("first_name", "last_name", "entity_name", "city", "state")
    csv_import_url_name = "principalsinformation" 
    def get_import_view(self):
        def importprincipalsinformation(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
                reader = csv.DictReader(csv_file)
                try:
                    batch_size = getattr(settings, 'BATCH_SIZE', 1000)
                    rows = list(reader)  # Convert to list for easy batching
                    total_rows = len(rows)
                    logger.info(f"{full_function_name}: Total number of records in the {request.FILES} : {total_rows}")
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
                                logger.info(f"{full_function_name}: Created new record with file_number {entity_num}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/principalsinformation/")  # This redirects to the changelist 
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
        return importprincipalsinformation
    


@admin.register(LicenseeNameDataEnrichment)
class FilingsInformationAdmin(CSVImportAdminMixin, admin.ModelAdmin):
    list_display = ("license_number", "name", "role","licensee", "last_name", "first_name")
    csv_import_url_name = "licensee_name_dataenrichment" 
    def get_import_view(self):
        def importlicenseenamedataenrichment(request):
            full_function_name = get_full_function_name()
            if request.method == "POST":
                form = CSVImportForm(request.POST, request.FILES)
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8",errors='replace')
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
                                logger.info(f"{full_function_name}: Created new record with file_number {licenseNumber}.")
                            except Exception as e:
                                logger.error(f"{full_function_name}: Error importing CSV: {str(e)}")
                                self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
                        logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                    time.sleep(10) 
                    logger.info(f"{full_function_name}: CSV imported successfully!")
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                    return HttpResponseRedirect("/admin/core_app/principalsinformation/")  # This redirects to the changelist 
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
        return importlicenseenamedataenrichment