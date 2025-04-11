import re
import ahocorasick
from django.contrib import admin
from ABC_BizEnrichment.common.core_app.helper_function import BaseCSVImportAdmin
from ABC_BizEnrichment.common.helper_function import get_column_names, parse_date, validate_yelp_rating
from core_app.models import AgentsInformation, CompanyInformationRecord, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import DataSet1Record
from django.contrib import admin
from django.db.models import Min
from django.contrib import messages
from django.db.models import Value, F, CharField
from django.db.models.functions import Replace, Lower
from django.db.models import CharField, Value
from django.db.models.functions import Lower, Replace, Cast
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('import_logs_of_data_set_2.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
# Genrating Data Set 1  Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # Licens eOutput Records 
@admin.register(LicenseOutput)
class LicenseOutputAdmin(BaseCSVImportAdmin):
    list_display = ("id", "abc_license_number", "abc_licensee", "abc_primary_owner", "abc_business_name","google_business_name", "google_business_status")
    # Step 1: Get IDs to keep

    csv_import_url_name = "license_output"
    fieldsets = (
        ('ABC License Records', {
            'classes': ('collapse',),
            'fields': ('abc_license_number', 'abc_primary_owner', 'abc_office_of_application', 'abc_business_name', 'abc_business_address', 'abc_county', 'abc_census_tract', 'abc_licensee', 'abc_license_type', 'abc_license_type_status', 'abc_status_date', 'abc_term', 'abc_original_issue_date', 'abc_expiration_date', 'abc_master', 'abc_duplicate', 'abc_fee_code', 'abc_transfers', 'abc_conditions', 'abc_operating_restrictions', 'abc_disciplinary_action', 'abc_disciplinary_history', 'abc_holds', 'abc_escrows', 'abc_from_license_number', 'abc_transferred_on', 'abc_to_license_number', 'abc_transferred_on2'),
        }),('Google Scrapp Record', {
            'classes': ('collapse',''),
            'fields': ('google_business_name', 'google_business_address','google_place_name','google_rating','google_phone_number','google_website','google_types','google_business_status'),
        }),
    )
    search_fields = ("abc_primary_owner","id","abc_license_number")
    def get_import_view(self):
        def importlicensenumber(request):
            mappings = {
                'abc_license_number': lambda row: row['license_number'],
                'abc_primary_owner': lambda row: row['primary_owner'],
                'abc_office_of_application': lambda row: row['office_of_application'],
                'abc_business_name': lambda row: row['business_name'],
                'abc_business_address': lambda row: row['business_address'],
                'abc_county': lambda row: row['county'],
                'abc_census_tract': lambda row: float(row['census_tract']) if row['census_tract'] else None,
                'abc_licensee': lambda row: row['licensee'],
                'abc_license_type': lambda row: row['license_type'],
                'abc_license_type_status': lambda row: row['license_type_status'],
                'abc_status_date': lambda row: parse_date(row['status_date']),
                'abc_original_issue_date': lambda row: parse_date(row['original_issue_date']),
                'abc_expiration_date': lambda row: parse_date(row['expiration_date']),
                'abc_term': lambda row: row['term'],
                'abc_master': lambda row: row['master'],
                'abc_duplicate': lambda row: int(row['duplicate']) if row['duplicate'] else None,
                'abc_fee_code': lambda row: row['fee_code'],
                'abc_transfers': lambda row: row['transfers'],
                'abc_conditions': lambda row: row['conditions'],
                'abc_operating_restrictions': lambda row: row['operating_restrictions'],
                'abc_disciplinary_action': lambda row: row['disciplinary_action'],
                'abc_disciplinary_history': lambda row: row['disciplinary_history'],
                'abc_holds': lambda row: row['holds'],
                'abc_escrows': lambda row: row['escrows'],
                'abc_transferred_on': lambda row: parse_date(row['transferred_on']),
                'abc_to_license_number': lambda row: row['to_license_number'],
                'abc_transferred_on2': lambda row: parse_date(row['transferred_on2']),
                # Goole Scrapping Data
                'google_business_name': lambda row: row['business_name_alt'] if row['business_name_alt'] else 'N/A',
                'google_business_address': lambda row: row['business_address_alt'] if row['business_address_alt'] else 'N/A',
                'google_place_name': lambda row: row['place_name'],
                'google_rating': lambda row: row['rating'],
                'google_phone_number': lambda row: row['phone_number'],
                'google_website': lambda row: row['website'],
                'google_types': lambda row: row['types'],
                'google_business_status': lambda row: row['business_status']
            }
            return self.process_csv_import(request, LicenseOutput, mappings)
        return importlicensenumber
    
# Yelp Restaurant Record
@admin.register(YelpRestaurantRecord)
class YelpRestaurantRecordAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "yelp_data"
    search_fields = ("yelp_license_type",)
    list_display = ("id","yelp_license_type", "yelp_file_number", "yelp_primary_name", "yelp_dba_name", "yelp_prem_addr_1", "yelp_prem_city", "yelp_prem_state", "yelp_prem_zip", "yelp_rating")
    yelp_output_all_columns = get_column_names(YelpRestaurantRecord,['id'], include_relations=True)
    fieldsets = (
        ('Yelp Restaurant Record', {
            'fields': tuple(yelp_output_all_columns),
        }),
    )
    def get_import_view(self):
        def ImportYelpRestaurantData(request):
            mappings = {
            'yelp_license_type': lambda row: row.get('License Type', '').strip(),
            'yelp_file_number': lambda row: row.get('File Number', '').strip(),
            'yelp_primary_name': lambda row: (row.get('Primary Name') or '').strip(),
            'yelp_dba_name': lambda row: (row.get('DBA Name') or '').strip(),
            'yelp_prem_addr_1': lambda row: (row.get('Prem Addr 1') or '').strip(),
            'yelp_prem_addr_2': lambda row: (row.get('Prem Addr 2') or '').strip(),
            'yelp_prem_city': lambda row: (row.get('Prem City') or '').strip(),
            'yelp_prem_state': lambda row: (row.get('Prem State') or '').strip(),
            'yelp_prem_zip': lambda row: (row.get('Prem Zip') or '').strip(),
            'yelp_link': lambda row: (row.get('Yelp Link') or '').strip(),
            'yelp_name': lambda row: (row.get('Yelp Name') or '').strip(),
            'yelp_phone': lambda row: (row.get('Yelp Phone') or '').strip(),
            'yelp_web_site': lambda row: (row.get('Yelp Web Site') or '').strip(),
            'yelp_rating': lambda row: validate_yelp_rating(row.get('Yelp Rating', '')), }
            return self.process_csv_import(request, YelpRestaurantRecord, mappings)
        return ImportYelpRestaurantData

@admin.register(CompanyInformationRecord)
class CompanyInformationRecordAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "company_info_data"
    search_fields = ("Company_Info_License_Number",)
    list_display = ("id","Company_Info_License_Number", "Company_Info_Type", "Company_Info_Name", "Company_Info_Role")
    yelp_output_all_columns = get_column_names(CompanyInformationRecord,['id'], include_relations=True)
    fieldsets = (
        ('Company Information Record', {
            'fields': tuple(yelp_output_all_columns),
        }),
    )
    def get_import_view(self):
        def ImportYelpRestaurantData(request):
            mappings = {
            'Company_Info_License_Number': lambda row: row.get('License Number', '').strip(),
            'Company_Info_Type': lambda row: row.get('Type', '').strip(),
            'Company_Info_Name': lambda row: row.get('Name', '').strip(),
            'Company_Info_Role': lambda row: row.get('Role', '').strip(),
            }
            return self.process_csv_import(request, CompanyInformationRecord, mappings)
        return ImportYelpRestaurantData

# Genrating Data Set 1  End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Genrating Data Set 2  Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@admin.register(PrincipalsInformation)
class PrincipalsInformationAdmin(BaseCSVImportAdmin):
    search_fields = ("principalsInformation_entity_num","principalsInformation_entity_name")
    list_display = ("id","principalsInformation_entity_num","principalsInformation_entity_name","principalsInformation_first_name", "principalsInformation_last_name", "principalsInformation_entity_name", "principalsInformation_city", "principalsInformation_state")
    csv_import_url_name = "principalsinformation" 
    PrincipalsInformationCoulmne = get_column_names(PrincipalsInformation,['id'], include_relations=True)
    fieldsets = (
        ('Principals Information Record', {
            'fields': tuple(PrincipalsInformationCoulmne),
        }),
    )
    def get_import_view(self):
        def importprincipalsinformation(request):
            mappings = {
                'principalsInformation_entity_name': lambda row: row['ENTITY_NAME'],
                'principalsInformation_entity_num': lambda row: row['ENTITY_NUM'],
                'principalsInformation_org_name': lambda row: row['ORG_NAME'],
                'principalsInformation_first_name': lambda row: row['FIRST_NAME'],
                'principalsInformation_middle_name': lambda row: row['MIDDLE_NAME'],
                'principalsInformation_last_name': lambda row: row['LAST_NAME'],
                'principalsInformation_address1': lambda row: row['ADDRESS1'],
                'principalsInformation_address2': lambda row: row['ADDRESS2'],
                'principalsInformation_address3': lambda row: row['ADDRESS3'],
                'principalsInformation_city': lambda row: row['CITY'],
                'principalsInformation_state': lambda row: row['STATE'],
                'principalsInformation_country': lambda row: row['COUNTRY'],
                'principalsInformation_postal_code': lambda row: row['POSTAL_CODE'],
                'principalsInformation_position_type': lambda row: row['POSITION_TYPE'],
                # 'principalsInformation_position_2': lambda row: row['POSITION2'],
                # 'principalsInformation_position_3': lambda row: row['POSITION3'],
                # 'principalsInformation_position_4': lambda row: row['POSITION4'],
                # 'principalsInformation_position_5': lambda row: row['POSITION5'],
                # 'principalsInformation_position_6': lambda row: row['POSITION6'],
                # 'principalsInformation_position_7': lambda row: row['POSITION7'],
            }
            return self.process_csv_import(request, PrincipalsInformation, mappings)
        return importprincipalsinformation
    
    
@admin.register(AgentsInformation)
class AgentsInformationAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "agentsinformation"
    search_fields = ("agentsInformation_entity_num","agentsInformation_entity_name")
    list_display = ("id","agentsInformation_entity_num","agentsInformation_entity_name")
    PrincipalsInformationCoulmne = get_column_names(AgentsInformation,['id'], include_relations=True)
    fieldsets = (
        ('Agents Information Record', {
            'fields': tuple(PrincipalsInformationCoulmne),
        }),
    )
    def get_import_view(self):
        def importagentsinformation(request):
            mappings = {
                'agentsInformation_entity_num': lambda row: row['ENTITY_NUM'],
                'agentsInformation_entity_name': lambda row: row['ENTITY_NAME'],
                'agentsInformation_org_name': lambda row: row['ORG_NAME'],
                'agentsInformation_first_name': lambda row: row['FIRST_NAME'],
                'agentsInformation_middle_name': lambda row: row['MIDDLE_NAME'],
                'agentsInformation_last_name': lambda row: row['LAST_NAME'],
                'agentsInformation_physical_address1': lambda row: row['PHYSICAL_ADDRESS1'],
                'agentsInformation_physical_address2': lambda row: row['PHYSICAL_ADDRESS2'],
                'agentsInformation_physical_address3': lambda row: row['PHYSICAL_ADDRESS3'],
                'agentsInformation_physical_city': lambda row: row['PHYSICAL_CITY'],
                'agentsInformation_physical_state': lambda row: row['PHYSICAL_STATE'],
                'agentsInformation_physical_country': lambda row: row['PHYSICAL_COUNTRY'],
                'agentsInformation_physical_postal_code': lambda row: row['PHYSICAL_POSTAL_CODE'],
                'agentsInformation_agent_type': lambda row: row['AGENT_TYPE'],
            }
            return self.process_csv_import(request, AgentsInformation, mappings)
        return importagentsinformation

@admin.register(FilingsInformation)
class FilingsInformationAdmin(BaseCSVImportAdmin):
    csv_import_url_name = "filingsinformation"
    list_display = ("id", "filingsInformation_entity_num", "filingsInformation_entity_name")
    PrincipalsInformationCoulmne = get_column_names(FilingsInformation, ['id'], include_relations=True)  # type: ignore

    fieldsets = (
        ('Filings Information Record', {
            'fields': tuple(PrincipalsInformationCoulmne),
        }),
    )

    def get_import_view(self):
        def importfilingsinformation(request):
            def normalize_name(name):
                # Keep only letters (ignore digits, spaces, special chars), convert to uppercase
                return re.sub(r'[^A-Z0-9]', '', name.upper())

            print("📦 Preparing dataset...")
            
            print(len(DataSet1Record.objects.all()),"--------------->>>>>>>>")

            norm_dataset = [
                normalize_name(licensee)
                for licensee in DataSet1Record.objects.values_list('abc_licensee', flat=True)
                if licensee
            ]

            print(f"📈 Total licensees to load into automaton: {len(norm_dataset)}")

            match_count = {'matched': 0, 'unmatched': 0}

            mappings = {
                'filingsInformation_entity_name': lambda row: row['ENTITY_NAME'],
                'filingsInformation_entity_num': lambda row: row['ENTITY_NUM'],
                'filingsInformation_initial_filing_date': lambda row: parse_date(row['INITIAL_FILING_DATE']),
                'filingsInformation_jurisdiction': lambda row: row['JURISDICTION'],
                'filingsInformation_entity_status': lambda row: row['ENTITY_STATUS'],
                'filingsInformation_standing_sos': lambda row: row['STANDING_SOS'],
                'filingsInformation_entity_type': lambda row: row['ENTITY_TYPE'],
                'filingsInformation_filing_type': lambda row: row['FILING_TYPE'],
                'filingsInformation_foreign_name': lambda row: row['FOREIGN_NAME'],
                'filingsInformation_standing_ftb': lambda row: row['STANDING_FTB'],
                'filingsInformation_standing_vcfcf': lambda row: row['STANDING_VCFCF'],
                'filingsInformation_standing_agent': lambda row: row['STANDING_AGENT'],
                'filingsInformation_suspension_date': lambda row: row['SUSPENSION_DATE'],
                'filingsInformation_last_si_file_number': lambda row: row['LAST_SI_FILE_NUMBER'],
                'filingsInformation_last_si_file_date': lambda row: row['LAST_SI_FILE_DATE'],
                'filingsInformation_principal_address': lambda row: row['PRINCIPAL_ADDRESS'],
                'filingsInformation_principal_address2': lambda row: row['PRINCIPAL_ADDRESS2'],
                'filingsInformation_principal_city': lambda row: row['PRINCIPAL_CITY'],
                'filingsInformation_principal_state': lambda row: row['PRINCIPAL_STATE'],
                'filingsInformation_principal_country': lambda row: row['PRINCIPAL_COUNTRY'],
                'filingsInformation_principal_postal_code': lambda row: row['PRINCIPAL_POSTAL_CODE'],
                'filingsInformation_mailing_address': lambda row: row['MAILING_ADDRESS'],
                'filingsInformation_mailing_address2': lambda row: row['MAILING_ADDRESS2'],
                'filingsInformation_mailing_city': lambda row: row['MAILING_CITY'],
                'filingsInformation_mailing_state': lambda row: row['MAILING_STATE'],
                'filingsInformation_mailing_country': lambda row: row['MAILING_COUNTRY'],
                'filingsInformation_mailing_postal_code': lambda row: row['MAILING_POSTAL_CODE'],
                'filingsInformation_principal_address_in_ca': lambda row: row['PRINCIPAL_ADDRESS_IN_CA'],
                'filingsInformation_principal_address2_in_ca': lambda row: row['PRINCIPAL_ADDRESS2_IN_CA'],
                'filingsInformation_principal_city_in_ca': lambda row: row['PRINCIPAL_CITY_IN_CA'],
                'filingsInformation_principal_state_in_ca': lambda row: row['PRINCIPAL_STATE_IN_CA'],
                'filingsInformation_principal_country_in_ca': lambda row: row['PRINCIPAL_COUNTRY_IN_CA'],
                'filingsInformation_principal_postal_code_in_ca': lambda row: row['PRINCIPAL_POSTAL_CODE_IN_CA'],
                'filingsInformation_llc_management_structure': lambda row: row['LLC_MANAGEMENT_STRUCTURE'],
                'filingsInformation_type_of_business': lambda row: row['TYPE_OF_BUSINESS'],
            }

            def filter_func(row):
                raw_name = row.get('ENTITY_NAME', '')
                normalized_entity = normalize_name(raw_name)

                if normalized_entity in norm_dataset:
                    match_count['matched'] += 1
                    print(f"✅ MATCHED: ENTITY_NAME = '{raw_name}'")
                    return True
                else:
                    match_count['unmatched'] += 1
                    print(f"❌ UNMATCHED: ENTITY_NAME = '{raw_name}'")
                    return False

            print("🚀 Starting CSV Import with Automaton Matching...")
            result = self.process_csv_import(request, FilingsInformation, mappings, filter_func=filter_func)
            print("🏁 Import Summary")
            print(f"✅ Matched: {match_count['matched']}")
            print(f"❌ Unmatched: {match_count['unmatched']}")
            print("===========================================")
            return result

        return importfilingsinformation
# # Genrating Data Set 2  End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
