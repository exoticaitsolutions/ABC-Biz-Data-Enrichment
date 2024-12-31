from datetime import datetime
import logging
import os
from django.contrib import admin
from ABC_BizEnrichment.common.core_app.helper_function import BaseCSVImportAdmin, get_or_create_license_number
from ABC_BizEnrichment.common.helper_function import parse_date
from core_app.models import *

# Logging setup
LOG_FOLDER = f"storage/logs/{datetime.now().strftime('%Y%m%d')}"
os.makedirs(
    LOG_FOLDER, exist_ok=True
)  #
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"{LOG_FOLDER}/core_app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())


@admin.register(LicenseNumber)
class LicenseNumberAdmin(BaseCSVImportAdmin):
    list_display = ['id', 'license_number']
    actions = ['licenses_csv']
    csv_import_url_name = "licenses"
    def get_import_view(self):
        def importlicensenumber(request):
            mappings = {
                'license_number': lambda row:   row['License Numbers']
            }
            return self.process_csv_import(request, LicenseNumber, mappings)
        return importlicensenumber
    
# Genrating Data Set 1  Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Licens eOutput Records 
@admin.register(LicenseOutput)
class LicenseOutputAdmin(BaseCSVImportAdmin):
    list_display = ("id", "license_number", "licensee", "primary_owner", "business_name", "business_status")
    csv_import_url_name = "license_output"
    search_fields = ("primary_owner","id")
    def get_import_view(self):
        def importlicensenumber(request):
            mappings = {
                'license_number': lambda row: get_or_create_license_number(row['license_number']),
                'primary_owner': lambda row: row['primary_owner'],
                'office_of_application': lambda row: row['office_of_application'],
                'business_name': lambda row: row['business_name'],
                'business_address': lambda row: row['business_address'],
                'county': lambda row: row['county'],
                'census_tract': lambda row: float(row['census_tract']) if row['census_tract'] else None,
                'licensee': lambda row: row['licensee'],
                'license_type': lambda row: row['license_type'],
                'license_type_status': lambda row: row['license_type_status'],
                'status_date': lambda row: parse_date(row['status_date']),
                'original_issue_date': lambda row: parse_date(row['original_issue_date']),
                'expiration_date': lambda row: parse_date(row['expiration_date']),
                'term': lambda row: row['term'],
                'master': lambda row: row['master'],
                'duplicate': lambda row: int(row['duplicate']) if row['duplicate'] else None,
                'fee_code': lambda row: row['fee_code'],
                'transfers': lambda row: row['transfers'],
                'conditions': lambda row: row['conditions'],
                'operating_restrictions': lambda row: row['operating_restrictions'],
                'disciplinary_action': lambda row: row['disciplinary_action'],
                'disciplinary_history': lambda row: row['disciplinary_history'],
                'holds': lambda row: row['holds'],
                'escrows': lambda row: row['escrows'],
                'transferred_on': lambda row: parse_date(row['transferred_on']),
                'to_license_number': lambda row: row['to_license_number'],
                'transferred_on2': lambda row: parse_date(row['transferred_on2']),
                'business_name_alt': lambda row: row['business_name_alt'] if row['business_name_alt'] else 'N/A',
                'business_address_alt': lambda row: row['business_address_alt'] if row['business_address_alt'] else 'N/A',
                'place_name': lambda row: row['place_name'],
                'rating': lambda row: row['rating'],
                'phone_number': lambda row: row['phone_number'],
                'website': lambda row: row['website'],
                'types': lambda row: row['types'],
                'business_status': lambda row: row['business_status']
            }
            return self.process_csv_import(request, LicenseOutput, mappings)
        return importlicensenumber
    
# Yelp Restaurant Record
@admin.register(YelpRestaurantRecord)
class YelpRestaurantRecordAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "yelp_data"
    search_fields = ("file_number__license_number",)
    list_display = ("id","license_type", "file_number", "primary_name", "dba_name", "prem_addr_1", "prem_city", "prem_state", "prem_zip", "yelp_rating")
    def get_import_view(self):
        def ImportYelpRestaurantData(request):
            mappings = {
                'license_type': lambda row: row['License Type'],
                'file_number': lambda row: get_or_create_license_number(row['File Number']),
                'primary_name': lambda row: row.get('Primary Name', '').strip(),
                'dba_name': lambda row: row['DBA Name'],
                'prem_addr_1': lambda row: row['Prem Addr 1'],
                'prem_addr_2': lambda row: row.get('Prem Addr 2', '').strip(),
                'prem_city': lambda row: row['Prem City'],
                'prem_state': lambda row: row['Prem State'],
                'prem_zip': lambda row: row['Prem Zip'],
                'yelp_link': lambda row: row.get('Yelp Link', '').strip(),
                'yelp_name': lambda row: row.get('Yelp Name', '').strip(),
                'yelp_phone': lambda row: row.get('Yelp Phone', '').strip(),
                'yelp_web_site': lambda row: row.get('Yelp Web Site', '').strip(),
                'yelp_rating': lambda row: row.get('Yelp Rating', '').strip()
            }
            return self.process_csv_import(request, YelpRestaurantRecord, mappings)
        return ImportYelpRestaurantData
    

# Genrating Data Set 1  End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Genrating Data Set 2  Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

@admin.register(AgentsInformation)
class AgentsInformationAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "agentsinformation"
    search_fields = ("entity_num",)
    list_display = ("id","entity_num","entity_name", "first_name", "last_name", "physical_city", "agent_type")
    def get_import_view(self):
        def importagentsinformation(request):
            mappings = {
                'entity_num': lambda row: row['ENTITY_NUM'],
                'entity_name': lambda row: row['ENTITY_NAME'],
                'org_name': lambda row: row['ORG_NAME'],
                'first_name': lambda row: row['FIRST_NAME'],
                'middle_name': lambda row: row['MIDDLE_NAME'],
                'last_name': lambda row: row['LAST_NAME'],
                'physical_address1': lambda row: row['PHYSICAL_ADDRESS1'],
                'physical_address2': lambda row: row['PHYSICAL_ADDRESS2'],
                'physical_address3': lambda row: row['PHYSICAL_ADDRESS3'],
                'physical_city': lambda row: row['PHYSICAL_CITY'],
                'physical_state': lambda row: row['PHYSICAL_STATE'],
                'physical_country': lambda row: row['PHYSICAL_COUNTRY'],
                'physical_postal_code': lambda row: row['PHYSICAL_POSTAL_CODE'],
                'agent_type': lambda row: row['AGENT_TYPE'],
            }
            return self.process_csv_import(request, AgentsInformation, mappings)
        return importagentsinformation


@admin.register(FilingsInformation)
class FilingsInformationAdmin(BaseCSVImportAdmin):
    search_fields = ("entity_num",)
    list_display = ("id","entity_num","file_number", "license_type", "type_status", "primary_name", "jurisdiction")
    csv_import_url_name = "filingsinformation" 
    def get_import_view(self):
        def importfilingsinformation(request):
            mappings = {
                'license_type': lambda row: row['License_Type'],
                'file_number': lambda row: row['File_Number'],
                'lic_or_app': lambda row: row['Lic_or_App'],
                'type_status': lambda row: row['Type_Status'],
                'type_orig_iss_date': lambda row: parse_date(row['Type_Orig_Iss_Date']),
                'expir_date': lambda row: parse_date(row['Expir_Date']),
                'fee_codes': lambda row: row['Fee_Codes'],
                'dup_counts': lambda row: row['Dup_Counts'],
                'master_ind': lambda row: row['Master_Ind'],
                'term_in_number_of_months': lambda row: row['Term_in_#_of_Months'],
                'geo_code': lambda row: row['Geo_Code'],
                'district': lambda row: row['District'],
                'primary_name': lambda row: row['Primary_Name'],
                'prem_addr_1': lambda row: row['Prem_Addr_1'],
                'prem_addr_2': lambda row: row['Prem_Addr_2'],
                'prem_city': lambda row: row['Prem_City'],
                'prem_state': lambda row: row['Prem_State'],
                'prem_zip': lambda row: row['Prem_Zip'],
                'dba_name': lambda row: row['DBA_Name'],
                'mail_addr_1': lambda row: row['Mail_Addr_1'],
                'mail_addr_2': lambda row: row['Mail_Addr_2'],
                'mail_city': lambda row: row['Mail_City'],
                'mail_state': lambda row: row['Mail_State'],
                'mail_zip': lambda row: row['Mail_Zip'],
                'prem_county': lambda row: row['Prem_County'],
                'prem_census_tract': lambda row: row['Prem_Census_Tract_#'],
                'entity_name': lambda row: row['ENTITY_NAME'],
                'entity_num': lambda row: row['ENTITY_NUM'],
                'initial_filing_date': lambda row: parse_date(row['INITIAL_FILING_DATE']),
                'jurisdiction': lambda row: row['JURISDICTION'],
                'entity_status': lambda row: row['ENTITY_STATUS'],
                'standing_sos': lambda row: row['STANDING_SOS'],
                'entity_type': lambda row: row['ENTITY_TYPE'],
                'filing_type': lambda row: row['FILING_TYPE'],
                'foreign_name': lambda row: row['FOREIGN_NAME'],
                'standing_ftb': lambda row: row['STANDING_FTB'],
                'standing_ftb': lambda row: row['STANDING_VCFCF'],
                'standing_vcfcf': lambda row: row['STANDING_AGENT'],
                'standing_agent': lambda row: row['STANDING_AGENT'],
                'suspension_date': lambda row: parse_date(row['SUSPENSION_DATE']),
                'last_si_file_number': lambda row: row['LAST_SI_FILE_NUMBER'],
                'last_si_file_date': lambda row: parse_date(row['LAST_SI_FILE_DATE']),
                'principal_address': lambda row: row['PRINCIPAL_ADDRESS'],
                'principal_address2': lambda row: row['PRINCIPAL_ADDRESS2'],
                'principal_city': lambda row: row['PRINCIPAL_CITY'],
                'principal_state': lambda row: row['PRINCIPAL_STATE'],
                'principal_country': lambda row: row['PRINCIPAL_COUNTRY'],
                'principal_postal_code': lambda row: row['PRINCIPAL_POSTAL_CODE'],
                'mailing_address': lambda row: row['MAILING_ADDRESS'],
                'mailing_address2': lambda row: row['MAILING_ADDRESS2'],
                'mailing_city': lambda row: row['MAILING_CITY'],
                'mailing_state': lambda row: row['MAILING_STATE'],
                'mailing_country': lambda row: row['MAILING_COUNTRY'],
                'mailing_postal_code': lambda row: row['MAILING_POSTAL_CODE'],
                'principal_address_in_ca': lambda row: row['PRINCIPAL_ADDRESS_IN_CA'],
                'principal_address2_in_ca': lambda row: row['PRINCIPAL_ADDRESS2_IN_CA'],
                'principal_city_in_ca': lambda row: row['PRINCIPAL_CITY_IN_CA'],
                'principal_state_in_ca': lambda row: row['PRINCIPAL_STATE_IN_CA'],
                'principal_country_in_ca': lambda row: row['PRINCIPAL_COUNTRY_IN_CA'],
                'principal_postal_code_in_ca': lambda row: row['PRINCIPAL_POSTAL_CODE_IN_CA'],
                'llc_management_structure': lambda row: row['LLC_MANAGEMENT_STRUCTURE'],
                'type_of_business': lambda row: row['TYPE_OF_BUSINESS'],
            }
            return self.process_csv_import(request, FilingsInformation, mappings)
        return importfilingsinformation
    

@admin.register(PrincipalsInformation)
class PrincipalsInformationAdmin(BaseCSVImportAdmin):
    search_fields = ("entity_num",)
    list_display = ("id","entity_num","entity_name","first_name", "last_name", "entity_name", "city", "state")
    csv_import_url_name = "principalsinformation" 
    def get_import_view(self):
        def importprincipalsinformation(request):
            mappings = {
                'entity_name': lambda row: row['ENTITY_NAME'],
                'entity_num': lambda row: row['ENTITY_NUM'],
                'org_name': lambda row: row['ORG_NAME'],
                'first_name': lambda row: row['FIRST_NAME'],
                'middle_name': lambda row: row['MIDDLE_NAME'],
                'last_name': lambda row: row['LAST_NAME'],
                'position_type': lambda row: row['POSITION_TYPE'],
                'address1': lambda row: row['ADDRESS1'],
                'address2': lambda row: row['ADDRESS2'],
                'address3': lambda row: row['ADDRESS3'],
                'city': lambda row: row['CITY'],
                'state': lambda row: row['STATE'],
                'country': lambda row: row['COUNTRY'],
                'postal_code': lambda row: row['POSTAL_CODE'],
                'principal_address': lambda row: row['PRINCIPAL_ADDRESS'],
                'principal_address2': lambda row: row['PRINCIPAL_ADDRESS2'],
                'principal_city': lambda row: row['PRINCIPAL_CITY'],
                'principal_state': lambda row: row['PRINCIPAL_STATE'],
                'principal_country': lambda row: row['PRINCIPAL_COUNTRY'],
                'principal_postal_code': lambda row: row['PRINCIPAL_POSTAL_CODE'],
                'principal_address_in_ca': lambda row: row['PRINCIPAL_ADDRESS_IN_CA'],
                'principal_address2_in_ca': lambda row: row['PRINCIPAL_ADDRESS2_IN_CA'],
                'principal_city_in_ca': lambda row: row['PRINCIPAL_CITY_IN_CA'],
                'principal_state_in_ca': lambda row: row['PRINCIPAL_STATE_IN_CA'],
                'principal_country_in_ca': lambda row: row['PRINCIPAL_COUNTRY_IN_CA'],
                'principal_postal_code_in_ca': lambda row: row['PRINCIPAL_POSTAL_CODE_IN_CA'],
                'position_1': lambda row: row['POSITION1'],
                'position_2': lambda row: row['POSITION2'],
                'position_3': lambda row: row['POSITION3'],
                'position_4': lambda row: row['POSITION4'],
                'position_5': lambda row: row['POSITION5'],
                'position_6': lambda row: row['POSITION6'],
                'position_7': lambda row: row['POSITION7'],
                'position_8': lambda row: row['POSITION8'],
            }
            return self.process_csv_import(request, PrincipalsInformation, mappings)
        return importprincipalsinformation
# Genrating Data Set 2  End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
