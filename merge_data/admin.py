
import csv
import time
from django.contrib import admin,messages
from django.db.models import CharField
from ABC_BizEnrichment.common.helper_function import generate_model_fields, get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from ABC_BizEnrichment.common.logconfig import logger
from django.db.models import F, Value
from django.db.models.functions import Replace, Lower
from core_app.models import AgentsInformation, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import DataErichment, DataSet1Record, DataSet2Record
from import_export.admin import ExportMixin
import pandas as pd

@admin.register(DataSet1Record)
class DataSet1RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):  # ExportMixin
    merge_url_name = "export_data_set1"
    actions = ["merge_and_import_data_action"]
    search_fields = ['abc_license_number', 'yelp_file_number', "abc_licensee", "yelp_primary_name"]
    list_display = ("id", "abc_license_number", "yelp_file_number", "abc_licensee", "yelp_primary_name", 
                    "output_license_file_status", "yelp_file_status")
    
    license_output_all_columns = get_column_names(LicenseOutput, ['id'], include_relations=True)
    yelp_output_all_columns = get_column_names(YelpRestaurantRecord, ['id'], include_relations=True)
    
    fieldsets = (
        ('License Output Record', {
            'fields': tuple(license_output_all_columns),
        }),
        ('Yelp Restaurant Record', {
            'fields': tuple(yelp_output_all_columns),
        }),
        ('Additional Fields Record', {
            'fields': ('output_license_file_status', 'yelp_file_status'),
        }),
    )

    def get_merge_view(self):
        def MergeDataSet1RecordsAdmin(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for LicenseOutput & AbcBizYelpRestaurantData and saved in the Dataset 1 (Business License)'
            batch_size = 300
            license_output_qs = YelpRestaurantRecord.objects.all()
            total_license_count = license_output_qs.count()
            dataset1_records_to_create = []
            logger.info(f"{full_function_name}: total number of Yelp Restaurant Records data: {total_license_count}")

            for i in range(0, total_license_count, batch_size):
                batch_license_output_ = license_output_qs[i:i + batch_size]
                for batch_license_output_data in batch_license_output_:
                    dataset1records = DataSet1Record()
                    license_number = batch_license_output_data.yelp_file_number
                    logger.info(f"{full_function_name}: Processing license_number: {license_number}")
                    
                    yelp_output = LicenseOutput.objects.filter(abc_license_number=license_number).first()
                    if yelp_output:
                        logger.info(f"{full_function_name}: Records Found in the yelp_output {license_number}")
                        for column_nameq in self.license_output_all_columns:
                            if hasattr(yelp_output, column_nameq):
                                setattr(dataset1records, column_nameq, getattr(yelp_output, column_nameq))
                        dataset1records.output_license_file_status = True
                        for column_name in self.yelp_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                        dataset1records.yelp_file_status = True
                    else:
                        logger.info(f"{full_function_name}: Records are not found in the yelp_output {license_number}")
                        for column_name in self.yelp_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                        dataset1records.yelp_file_status = True
                    
                    dataset1_records_to_create.append(dataset1records)
                    
                logger.info(f"{full_function_name}: Imported {len(batch_license_output_)} records (batch {i // batch_size + 1}).")
                logger.info(f"{full_function_name}: {dataset1_records_to_create}")
                if dataset1_records_to_create:
                    DataSet1Record.objects.bulk_create(dataset1_records_to_create)
                    logger.info(f"{full_function_name}: Imported {len(dataset1_records_to_create)} records.")
                    dataset1_records_to_create.clear()
                    time.sleep(2)
                # break
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset1record/")
        
        return MergeDataSet1RecordsAdmin

    

@admin.register(DataSet2Record)
class DataSet2RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "data_set2_records"
    agents_information_all_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    principalsinformation_all_columns = get_column_names(PrincipalsInformation, ['id'], include_relations=True)
    data_all_columns = get_column_names(DataErichment, ['id'], include_relations=True)

    fieldsets = (
         ('Principals Information Record', {
            'classes': ('collapse',),
 
            'fields': tuple(principalsinformation_all_columns),
        }),
        ('Agents Information Record', {
            'classes': ('collapse',),
            'fields': tuple(agents_information_all_columns),
        }),
        ('Filings Information Record', {
            'classes': ('collapse',),   
            'fields': tuple(filings_Information_all_columns),
        }),
        ('Additional Fields Record', {
            'fields': ('filling_information_file_status', 'principal_information_file_status','agentsInformation_file_status'),
        }),
    )
    list_display = ("id","principalsInformation_entity_num","principalsInformation_entity_name","principalsInformation_first_name","principalsInformation_last_name","filling_information_file_status","principal_information_file_status","agentsInformation_file_status")
    search_fields = ['principalsInformation_entity_name']  # You can add other fields here as needed
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            batch_size = 10000
            fetch_principalsinformation = PrincipalsInformation.objects.all()
            total_principalsinformation_count = fetch_principalsinformation.count()
            logger.info(f"{full_function_name}: total number of Filings Information data: {total_principalsinformation_count}")
            # # Initialize a list to hold dataset2records instances for bulk create
            records_to_create = []
            for i in range(0, total_principalsinformation_count, batch_size):
                batch_principalsinformation = fetch_principalsinformation[i:i + batch_size]
                for principalsinformationdata in batch_principalsinformation:
                    entity_name = str(principalsinformationdata.principalsInformation_entity_name)
                    logger.info(f"{full_function_name}: entity_name = {entity_name}")
                    dataset2records = DataSet2Record()
                    agent_informations = AgentsInformation.objects.filter(agentsInformation_entity_name=entity_name).first()
                    filingsinformation = FilingsInformation.objects.filter(filingsInformation_entity_name=entity_name).first()
                    if agent_informations:
                        logger.info(f"{full_function_name}: Records Found in the Agents Information")
                        # If found in the Agents Information, copy the data to the dataset2records instance
                        for column_name in self.agents_information_all_columns:
                            if hasattr(agent_informations, column_name):
                                setattr(dataset2records, column_name, getattr(agent_informations, column_name))
                            dataset2records.agentsInformation_file_status = True
                    if filingsinformation:
                        for column_name in self.filings_Information_all_columns:
                            if hasattr(filingsinformation, column_name):
                                setattr(dataset2records, column_name, getattr(filingsinformation, column_name))
                            dataset2records.filling_information_file_status = True
                        logger.info(f"{full_function_name}: Records Found in the filings information")

                    logger.info(f"{full_function_name}: Records Found no found in the filings information")
                    for column_name in self.principalsinformation_all_columns:
                        if hasattr(principalsinformationdata, column_name):
                            setattr(dataset2records, column_name, getattr(principalsinformationdata, column_name))
                        dataset2records.principal_information_file_status = True
                    records_to_create.append(dataset2records)    
                logger.info(f"{full_function_name}: (records_to_create) = {records_to_create}")
                if dataset2records:
                    DataSet2Record.objects.bulk_create(records_to_create)
                    logger.info(f"{full_function_name}: Imported {len(records_to_create)} records (batch {i // batch_size + 1}).")
                    records_to_create.clear()
                    time.sleep(2)
                # break
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # Redirect after processing
        return DataSet2Recordmerge_view
@admin.register(DataErichment)
class DataErichmentAdmin(ExportMixin, admin.ModelAdmin):
    merge_url_name = "dataerichmentrecords"
    list_display = ("id","abc_licensee", "agentsInformation_entity_name", "data_set_1_file_status", "data_set_2_file_status","Company_Info_Name","Company_Info_Type")
    # search_fields = ['entity_name', 'licensee']  # Add more fields here if needed
    def get_merge_view(self):
        def DataSet3Recordmerge_view(request):
            full_function_name = get_full_function_name()
            data_all_columns = get_column_names(DataErichment,['id'], include_relations=True) # type: ignore
            message = 'Data Merged successfully for Filling , Principal & Agent and saved in Dataset 2 (Combined Information)'
            csv_file = 'Mike_Data_Erichment_Records.csv'
            logger.info(f"{full_function_name}: Reading CSV file {csv_file}...")
            with open(csv_file, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
            logger.info(f"{full_function_name}: Total number of records in the {csv_file} : {len(rows)}")
            batch_size = 50
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                for batch_license_output_data in batch:
                    print(batch_license_output_data["Company_Info_License Number"])
                    # break
                    dataset2records = DataErichment()
                    dataset2records.abc_license_number = batch_license_output_data["abc_license_number"]
                    dataset2records.Company_Info_License_Number = batch_license_output_data["Company_Info_License Number"]
                    dataset2records.Company_Info_Type = batch_license_output_data["Company_Info_Type"]
                    dataset2records.Company_Info_Name = batch_license_output_data["Company_Info_Name"]
                    dataset2records.Company_Info_Role = batch_license_output_data["Company_Info_Role"]
                    dataset2records.abc_business_address = batch_license_output_data["abc_business_address"]
                    dataset2records.abc_county = batch_license_output_data["abc_county"]
                    dataset2records.abc_census_tract = batch_license_output_data["abc_census_tract"]
                    dataset2records.abc_licensee = batch_license_output_data["abc_licensee"]
                    dataset2records.abc_license_type = batch_license_output_data["abc_license_type"]
                    dataset2records.abc_license_type_status = batch_license_output_data["abc_license_type_status"]
                    dataset2records.abc_status_date = batch_license_output_data["abc_status_date"]
                    dataset2records.abc_term = batch_license_output_data["abc_term"]
                    dataset2records.abc_original_issue_date = batch_license_output_data["abc_original_issue_date"]
                    dataset2records.abc_expiration_date = batch_license_output_data["abc_expiration_date"]
                    dataset2records.abc_master = batch_license_output_data["abc_master"]
                    dataset2records.abc_duplicate = bool(batch_license_output_data["abc_duplicate"])
                    dataset2records.abc_fee_code = batch_license_output_data["abc_fee_code"]
                    dataset2records.abc_transfers = batch_license_output_data["abc_transfers"]
                    dataset2records.abc_conditions = batch_license_output_data["abc_conditions"]
                    dataset2records.abc_operating_restrictions = batch_license_output_data["abc_operating_restrictions"]
                    dataset2records.abc_disciplinary_action = batch_license_output_data["abc_disciplinary_action"]
                    dataset2records.abc_disciplinary_history = batch_license_output_data["abc_disciplinary_history"]
                    dataset2records.abc_holds = batch_license_output_data["abc_holds"]
                    dataset2records.abc_escrows = batch_license_output_data["abc_escrows"]
                    dataset2records.abc_from_license_number = batch_license_output_data["abc_from_license_number"]
                    dataset2records.abc_transferred_on = batch_license_output_data["abc_transferred_on"]
                    dataset2records.abc_to_license_number = batch_license_output_data["abc_to_license_number"]
                    dataset2records.abc_transferred_on2 = batch_license_output_data["abc_transferred_on2"]
                    dataset2records.abc_license_type = batch_license_output_data["abc_license_type"]
                    dataset2records.abc_file_number = batch_license_output_data["abc_file_number"]
                    dataset2records.abc_lic_or_app = batch_license_output_data["abc_lic_or_app"]
                    dataset2records.abc_type_status = batch_license_output_data["abc_type_status"]
                    dataset2records.abc_type_orig_iss_date = batch_license_output_data["abc_type_orig_iss_date"]
                    dataset2records.abc_expir_date = batch_license_output_data["abc_expir_date"]
                    dataset2records.abc_fee_codes = batch_license_output_data["abc_fee_codes"]
                    dataset2records.abc_dup_counts = batch_license_output_data["abc_dup_counts"]
                    dataset2records.abc_master_ind = batch_license_output_data["abc_master_ind"]
                    dataset2records.abc_term_in_number_of_months = batch_license_output_data["abc_term_in_number_of_months"]
                    dataset2records.abc_geo_code = batch_license_output_data["abc_geo_code"]
                    dataset2records.abc_district = batch_license_output_data["abc_district"]
                    dataset2records.abc_primary_name = batch_license_output_data["abc_primary_name"]
                    dataset2records.abc_prem_addr_1 = batch_license_output_data["abc_prem_addr_1"]
                    dataset2records.abc_prem_addr_2 = batch_license_output_data["abc_prem_addr_2"]
                    dataset2records.abc_prem_city = batch_license_output_data["abc_prem_city"]
                    dataset2records.abc_prem_state = batch_license_output_data["abc_prem_state"]
                    dataset2records.abc_prem_zip = batch_license_output_data["abc_prem_zip"]
                    dataset2records.abc_dba_name = batch_license_output_data["abc_dba_name"]
                    dataset2records.abc_mail_addr_1 = batch_license_output_data["abc_mail_addr_1"]
                    dataset2records.abc_mail_addr_2 = batch_license_output_data["abc_mail_addr_2"]
                    dataset2records.abc_mail_city = batch_license_output_data["abc_mail_city"]
                    dataset2records.abc_mail_state = batch_license_output_data["abc_mail_state"]
                    dataset2records.abc_mail_zip = batch_license_output_data["abc_mail_zip"]
                    dataset2records.abc_prem_county = batch_license_output_data["abc_prem_county"]
                    dataset2records.abc_prem_census_tract = batch_license_output_data["abc_prem_census_tract"] # type: ignore
                    dataset2records.google_business_name = batch_license_output_data["google_business_name"]
                    dataset2records.google_business_address = batch_license_output_data["google_business_address"]
                    dataset2records.google_place_name = batch_license_output_data["google_place_name"]
                    dataset2records.google_rating = batch_license_output_data["google_rating"]
                    dataset2records.google_phone_number = batch_license_output_data["google_phone_number"]
                    dataset2records.google_website = batch_license_output_data["google_website"]
                    dataset2records.google_types = batch_license_output_data["google_types"]
                    dataset2records.google_business_status = batch_license_output_data["google_business_status"]
                    dataset2records.yelp_file_number = batch_license_output_data["yelp_file_number"]
                    dataset2records.yelp_license_type = batch_license_output_data["yelp_license_type"]
                    dataset2records.yelp_primary_name = batch_license_output_data["yelp_primary_name"]
                    dataset2records.yelp_dba_name = batch_license_output_data["yelp_dba_name"]
                    dataset2records.yelp_prem_addr_1 = batch_license_output_data["yelp_prem_addr_1"]
                    dataset2records.yelp_prem_addr_2 = batch_license_output_data["yelp_prem_addr_2"]
                    dataset2records.yelp_prem_city = batch_license_output_data["yelp_prem_city"]
                    dataset2records.yelp_prem_state = batch_license_output_data["yelp_prem_state"]
                    dataset2records.yelp_prem_zip = batch_license_output_data["yelp_prem_zip"]
                    dataset2records.yelp_link = batch_license_output_data["yelp_link"]
                    dataset2records.yelp_name = batch_license_output_data["yelp_name"]
                    dataset2records.yelp_phone = batch_license_output_data["yelp_phone"]
                    dataset2records.yelp_web_site = batch_license_output_data["yelp_web_site"]
                    dataset2records.yelp_rating = batch_license_output_data["yelp_rating"]
                    dataset2records.output_license_file_status = bool(batch_license_output_data["output_license_file_status"])
                    dataset2records.yelp_file_status = bool(batch_license_output_data["yelp_file_status"])
                    dataset2records.agentsInformation_entity_name = batch_license_output_data["agentsInformation_entity_name"]
                    dataset2records.agentsInformation_entity_num = batch_license_output_data["agentsInformation_entity_num"]
                    dataset2records.agentsInformation_org_name = batch_license_output_data["agentsInformation_org_name"]
                    dataset2records.agentsInformation_first_name = batch_license_output_data["agentsInformation_first_name"]
                    dataset2records.agentsInformation_middle_name = batch_license_output_data["agentsInformation_middle_name"]
                    dataset2records.agentsInformation_last_name = batch_license_output_data["agentsInformation_last_name"]
                    dataset2records.agentsInformation_physical_address1 = batch_license_output_data["agentsInformation_physical_address1"]
                    dataset2records.agentsInformation_physical_address2 = batch_license_output_data["agentsInformation_physical_address2"]
                    dataset2records.agentsInformation_physical_address3 = batch_license_output_data["agentsInformation_physical_address3"]
                    dataset2records.agentsInformation_physical_city = batch_license_output_data["agentsInformation_physical_city"]
                    dataset2records.agentsInformation_physical_state = batch_license_output_data["agentsInformation_physical_state"]
                    dataset2records.agentsInformation_physical_country = batch_license_output_data["agentsInformation_physical_country"]
                    dataset2records.agentsInformation_physical_postal_code = batch_license_output_data["agentsInformation_physical_postal_code"]
                    dataset2records.agentsInformation_agent_type = batch_license_output_data["agentsInformation_agent_type"]
                    dataset2records.filingsInformation_entity_name = batch_license_output_data["filingsInformation_entity_name"]
                    dataset2records.filingsInformation_entity_num = batch_license_output_data["filingsInformation_entity_num"]
                    dataset2records.filingsInformation_initial_filing_date = batch_license_output_data["filingsInformation_initial_filing_date"]
                    dataset2records.filingsInformation_jurisdiction = batch_license_output_data["filingsInformation_jurisdiction"]
                    dataset2records.filingsInformation_entity_status = batch_license_output_data["filingsInformation_entity_status"]
                    dataset2records.filingsInformation_standing_sos = batch_license_output_data["filingsInformation_standing_sos"]
                    dataset2records.filingsInformation_entity_type = batch_license_output_data["filingsInformation_entity_type"]
                    dataset2records.filingsInformation_filing_type = batch_license_output_data["filingsInformation_filing_type"]
                    dataset2records.filingsInformation_foreign_name = batch_license_output_data["filingsInformation_foreign_name"]
                    dataset2records.filingsInformation_standing_ftb = batch_license_output_data["filingsInformation_standing_ftb"]
                    dataset2records.filingsInformation_standing_vcfcf = batch_license_output_data["filingsInformation_standing_vcfcf"]
                    dataset2records.filingsInformation_standing_agent = batch_license_output_data["filingsInformation_standing_agent"]
                    dataset2records.filingsInformation_suspension_date = batch_license_output_data["filingsInformation_suspension_date"]
                    dataset2records.filingsInformation_last_si_file_number = batch_license_output_data["filingsInformation_last_si_file_number"]
                    dataset2records.filingsInformation_last_si_file_date = batch_license_output_data["filingsInformation_last_si_file_date"]
                    dataset2records.filingsInformation_principal_address = batch_license_output_data["filingsInformation_principal_address"]
                    dataset2records.filingsInformation_principal_address2 = batch_license_output_data["filingsInformation_principal_address2"]
                    dataset2records.filingsInformation_principal_city = batch_license_output_data["filingsInformation_principal_city"]
                    dataset2records.filingsInformation_principal_state = batch_license_output_data["filingsInformation_principal_state"]
                    dataset2records.filingsInformation_principal_country = batch_license_output_data["filingsInformation_principal_country"]
                    dataset2records.filingsInformation_principal_postal_code = batch_license_output_data["filingsInformation_principal_postal_code"]
                    dataset2records.filingsInformation_mailing_address = batch_license_output_data["filingsInformation_mailing_address"]
                    dataset2records.filingsInformation_mailing_address2 = batch_license_output_data["filingsInformation_mailing_address2"]
                    dataset2records.filingsInformation_mailing_address3 = batch_license_output_data["filingsInformation_mailing_address3"]
                    dataset2records.filingsInformation_mailing_city = batch_license_output_data["filingsInformation_mailing_city"]
                    dataset2records.filingsInformation_mailing_state = batch_license_output_data["filingsInformation_mailing_state"]
                    dataset2records.filingsInformation_mailing_country = batch_license_output_data["filingsInformation_mailing_country"]
                    dataset2records.filingsInformation_mailing_postal_code = batch_license_output_data["filingsInformation_mailing_postal_code"]
                    dataset2records.filingsInformation_principal_address_in_ca = batch_license_output_data["filingsInformation_principal_address_in_ca"]
                    dataset2records.filingsInformation_principal_address2_in_ca = batch_license_output_data["filingsInformation_principal_address2_in_ca"]
                    dataset2records.filingsInformation_principal_city_in_ca = batch_license_output_data["filingsInformation_principal_city_in_ca"]
                    dataset2records.filingsInformation_principal_state_in_ca = batch_license_output_data["filingsInformation_principal_state_in_ca"]
                    dataset2records.filingsInformation_principal_country_in_ca = batch_license_output_data["filingsInformation_principal_country_in_ca"]
                    dataset2records.filingsInformation_principal_postal_code_in_ca = batch_license_output_data["filingsInformation_principal_postal_code_in_ca"]
                    dataset2records.filingsInformation_llc_management_structure = batch_license_output_data["filingsInformation_llc_management_structure"]
                    dataset2records.filingsInformation_type_of_business = batch_license_output_data["filingsInformation_type_of_business"]
                    dataset2records.principalsInformation_entity_name = batch_license_output_data["principalsInformation_entity_name"]
                    dataset2records.principalsInformation_entity_num = batch_license_output_data["principalsInformation_entity_num"]
                    dataset2records.principalsInformation_org_name = batch_license_output_data["principalsInformation_org_name"]
                    dataset2records.principalsInformation_first_name = batch_license_output_data["principalsInformation_first_name"]
                    dataset2records.principalsInformation_middle_name = batch_license_output_data["principalsInformation_middle_name"]
                    dataset2records.principalsInformation_last_name = batch_license_output_data["principalsInformation_last_name"]
                    dataset2records.principalsInformation_address1 = batch_license_output_data["principalsInformation_address1"]
                    dataset2records.principalsInformation_address2 = batch_license_output_data["principalsInformation_address2"]
                    dataset2records.principalsInformation_address3 = batch_license_output_data["principalsInformation_address3"]
                    dataset2records.principalsInformation_city = batch_license_output_data["principalsInformation_city"]
                    dataset2records.principalsInformation_state = batch_license_output_data["principalsInformation_state"]
                    dataset2records.principalsInformation_country = batch_license_output_data["principalsInformation_country"]
                    dataset2records.principalsInformation_postal_code = batch_license_output_data["principalsInformation_postal_code"]
                    dataset2records.principalsInformation_position_1 = batch_license_output_data["principalsInformation_position_1"]
                    dataset2records.principalsInformation_position_2 = batch_license_output_data["principalsInformation_position_2"]
                    dataset2records.principalsInformation_position_3 = batch_license_output_data["principalsInformation_position_3"]
                    dataset2records.principalsInformation_position_4 = batch_license_output_data["principalsInformation_position_4"]
                    dataset2records.principalsInformation_position_5 = batch_license_output_data["principalsInformation_position_5"]
                    dataset2records.principalsInformation_position_6 = batch_license_output_data["principalsInformation_position_6"]
                    dataset2records.principalsInformation_position_7 = batch_license_output_data["principalsInformation_position_7"]
                    dataset2records.filling_information_file_status = bool(batch_license_output_data["filling_information_file_status"])
                    dataset2records.principal_information_file_status =  bool(batch_license_output_data["principal_information_file_status"])
                    dataset2records.agentsInformation_file_status =  bool(batch_license_output_data["agentsInformation_file_status"])
                    dataset2records.data_set_1_file_status =  bool(batch_license_output_data["data_set_1_file_status"])
                    dataset2records.data_set_2_file_status =  bool(batch_license_output_data["data_set_2_file_status"])
                    dataset2records.save()    
                    # break
                break
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataerichment/")
        return DataSet3Recordmerge_view     