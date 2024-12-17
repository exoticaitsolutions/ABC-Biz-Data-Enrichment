
import logging
from datetime import datetime
import time
from django.contrib import admin,messages
from ABC_BizEnrichment.common.core_app.helper_function import get_or_create_license_number
from ABC_BizEnrichment.common.helper_function import get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from core_app.models import LicenseOutput, YelpRestaurantRecord
from merge_data.models import DataSet1Record 
# Logger setup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

@admin.register(DataSet1Record)
class DataSet1RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "export_data_set1"
    actions = ["merge_and_import_data_action"]
    search_fields = ['licensee']  # You can add other fields here as needed
    list_display = ("id", "file_number","license_number","business_name","primary_owner","primary_name","dba_name","output_license_file_status","yelp_file_status")
    def get_merge_view(self):
        def MergeDataSet1RecordsAdmin(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for LicenseOutput & AbcBizYelpRestaurantData and saved in the Dataset 1 (Business License)'
            # Get all column names (including relation fields)
            license_output_all_columns = get_column_names(LicenseOutput,['id', 'license_number'], include_relations=True)
            yelp_output_all_columns = get_column_names(YelpRestaurantRecord,['id', 'file_number'], include_relations=True)
            batch_size = 10000
            license_output_qs = LicenseOutput.objects.all()
            total_license_count = license_output_qs.count()
            logger.info(f"{full_function_name}: total number of BusinessLicense data: {total_license_count}")
            # Process in batches
            for i in range(0, total_license_count, batch_size):
                batch = license_output_qs[i:i+batch_size]  # Get a slice of the queryset
                for license_output_data in batch:
                    yelp_output = YelpRestaurantRecord.objects.filter(file_number=license_output_data.license_number).first()
                    dataset1records, created = DataSet1Record.objects.get_or_create(license_number=str(license_output_data.license_number))
                    if yelp_output:
                        print('yelp_output records  found')
                        for column_name in yelp_output_all_columns:
                            if hasattr(yelp_output, column_name):
                                setattr(dataset1records, column_name, getattr(yelp_output, column_name))
                        dataset1records.file_number = yelp_output.file_number
                        dataset1records.yelp_file_status = True
                    for column_name in license_output_all_columns:
                        if hasattr(license_output_data, column_name):
                            setattr(dataset1records, column_name, getattr(license_output_data, column_name))
                    dataset1records.output_license_file_status = True
                    dataset1records.save()
                    if created:
                        action = "Created"
                    else:
                        action = "Fetched"
                    logger.info(f"{full_function_name}: {action} BusinessLicense for license_number: {dataset1records.license_number}")
                logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                time.sleep(10)
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset1record/")  # This 
        return MergeDataSet1RecordsAdmin



# @admin.register(CombinedInformation)
# class CombinedInformationAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
#     merge_url_name = "informations_data"
#     list_display = ("entity_name", "entity_num", "first_name", "license_type","initial_filing_date")
#     search_fields = ['entity_name']  # You can add other fields here as needed
#     def get_merge_view(self):
#         def merge_view(request):
#             full_function_name = get_full_function_name()
#             batch_size = getattr(settings, 'BATCH_SIZE', 1000)
#             message = 'Data Merged successfully for Filling , Principal & Agent  and saved in the Dataset 2 (Combined Information)'
#             licensee_enrichment_data = PrincipalsInformation.objects.all()
#             total_license_count = licensee_enrichment_data.count()
#             logger.info(f"{full_function_name}: total number of BusinessLicense data: {total_license_count}")
#             for i in range(0, total_license_count, batch_size):
#                 batch = licensee_enrichment_data[i:i + batch_size]  # Fetch the current batch
#                 for singledata in batch:
#                     combined_information = CombinedInformation.objects.filter(entity_num=str(singledata.entity_num)).first() or CombinedInformation.objects.create(entity_num=str(singledata.entity_num))
#                     agent_informations = AgentsInformation.objects.filter(entity_num=str(singledata.entity_num)).first()
#                     print('agent_informations', agent_informations)
#                     filings_information = FilingsInformation.objects.filter(entity_num=str(singledata.entity_num)).first()
#                     print('filings_information', filings_information)
#                     if agent_informations:
#                         combined_information.entity_num = agent_informations.entity_num
#                         combined_information.entity_name = agent_informations.entity_name
#                         combined_information.org_name = agent_informations.org_name
#                         combined_information.first_name = agent_informations.first_name
#                         combined_information.middle_name = agent_informations.middle_name
#                         combined_information.last_name = agent_informations.last_name
#                         combined_information.physical_address1 = agent_informations.physical_address1
#                         combined_information.physical_address2 = agent_informations.physical_address2
#                         combined_information.physical_address3 = agent_informations.physical_address3
#                         combined_information.physical_city = agent_informations.physical_city
#                         combined_information.physical_state = agent_informations.physical_state
#                         combined_information.physical_country = agent_informations.physical_country
#                         combined_information.physical_postal_code = agent_informations.physical_postal_code
#                         combined_information.agent_type = agent_informations.agent_type
#                     if filings_information: 
#                         combined_information.entity_num = filings_information.entity_num
#                         combined_information.license_type = filings_information.license_type
#                         combined_information.file_number = filings_information.file_number
#                         combined_information.lic_or_app = filings_information.lic_or_app
#                         combined_information.type_status = filings_information.type_status
#                         combined_information.type_orig_iss_date  = filings_information.type_orig_iss_date
#                         combined_information.initial_filing_date = filings_information.initial_filing_date
#                         combined_information.expir_date = filings_information.expir_date
#                         combined_information.fee_codes = filings_information.fee_codes
#                         combined_information.dup_counts = filings_information.dup_counts
#                         combined_information.master_ind = filings_information.master_ind
#                         combined_information.term_in_number_of_months = filings_information.term_in_number_of_months
#                         combined_information.geo_code = filings_information.geo_code
#                         combined_information.district = filings_information.district
#                         combined_information.primary_name = filings_information.primary_name
#                         combined_information.prem_addr_1 = filings_information.prem_addr_1
#                         combined_information.prem_addr_2 = filings_information.prem_addr_2
#                         combined_information.prem_city = filings_information.prem_city
#                         combined_information.prem_state = filings_information.prem_state
#                         combined_information.prem_zip = filings_information.prem_zip
#                         combined_information.dba_name = filings_information.dba_name
#                         combined_information.mail_addr_1 = filings_information.mail_addr_1
#                         combined_information.mail_addr_2 = filings_information.mail_addr_2
#                         combined_information.mail_city = filings_information.mail_city
#                         combined_information.mail_state = filings_information.mail_state
#                         combined_information.mail_zip = filings_information.mail_zip
#                         combined_information.prem_county = filings_information.prem_county
#                         combined_information.prem_census_tract = filings_information.prem_census_tract
#                     combined_information.entity_num = singledata.entity_num
#                     combined_information.entity_name = singledata.entity_name
#                     combined_information.org_name = singledata.org_name
#                     combined_information.first_name = singledata.first_name
#                     combined_information.last_name = singledata.last_name
#                     combined_information.address1 = singledata.address1
#                     combined_information.address2 = singledata.address2
#                     combined_information.address3 = singledata.address3
#                     combined_information.city = singledata.city
#                     combined_information.state = singledata.state
#                     combined_information.country = singledata.country
#                     combined_information.postal_code = singledata.postal_code
#                     combined_information.position_1 = singledata.position_1
#                     combined_information.position_2 = singledata.position_2
#                     combined_information.position_3 = singledata.position_3
#                     combined_information.position_4 = singledata.position_4
#                     combined_information.position_5 = singledata.position_5
#                     combined_information.position_6 = singledata.position_6
#                     combined_information.position_7 = singledata.position_7
#                     combined_information.principal_file_status = True
#                     combined_information.save()  
#                     action =  "Created"
#                     logger.info(f"{full_function_name}: {action} CombinedInformation for entity_num: {combined_information.entity_num}")
#             logger.info(f"{full_function_name}: {message}")
#             self.message_user(request, message, messages.SUCCESS)
#             return HttpResponseRedirect("/admin/core_app/combinedinformation/")  # This 
#         return merge_view
    
# @admin.register(DataEnrichment)
# class DataEnrichmentAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
#     actions = ["merge_and_import_data_action"]
#     search_fields = ['business_name', 'license_number', 'city']  # Use valid field names
#     list_display = ("license_number","initial_filing_date","business_License_field_status","combine_information_filed_status","master","licensee","business_name","website","phone_number","last_name","first_name","middle_name")
    
#     merge_url_name = "enrichment_data"
#     def get_merge_view(self):
#         """
#         Subclass-specific view for handling the merge logic.
#         """
#         def merge_view(request):
#             businesslicense_data = BusinessLicense.objects.all()
#             total_license_count = businesslicense_data.count()
#             full_function_name = get_full_function_name()
        
#             message = 'Data Merged successfully for Data Set 1(Business License)   and  Data Set 2(Combined Information) saved in the Data Enrichment (Data Enrichment )'
#             logger.info(f"{full_function_name}: totat number of the BusinessLicense data: {total_license_count}")
#             for singledata in businesslicense_data:
#                 # dataenrichmentinformation = DataEnrichment()
#                 dataenrichmentinformation = DataEnrichment.objects.filter(license_number=str(singledata.license_number)).first() or DataEnrichment.objects.create(license_number=str(singledata.license_number))
#                 print('licensee', singledata.licensee)
#                 combinedinformation = CombinedInformation.objects.filter(entity_name__icontains=str(singledata.licensee)).first()
#                 print('combinedinformation', combinedinformation)
#                 if combinedinformation:
#                     dataenrichmentinformation.entity_num = combinedinformation.entity_num
#                     dataenrichmentinformation.org_name = combinedinformation.org_name
#                     dataenrichmentinformation.first_name = combinedinformation.first_name
#                     dataenrichmentinformation.middle_name = combinedinformation.middle_name
#                     dataenrichmentinformation.last_name = combinedinformation.last_name
#                     dataenrichmentinformation.physical_address1 = combinedinformation.physical_address1
#                     dataenrichmentinformation.physical_address2 = combinedinformation.physical_address2
#                     dataenrichmentinformation.physical_address3 = combinedinformation.physical_address3
#                     dataenrichmentinformation.physical_city = combinedinformation.physical_city
#                     dataenrichmentinformation.physical_state = combinedinformation.physical_state
#                     dataenrichmentinformation.physical_country = combinedinformation.physical_country
#                     dataenrichmentinformation.physical_postal_code = combinedinformation.physical_postal_code
#                     dataenrichmentinformation.agent_type = combinedinformation.agent_type
#                     dataenrichmentinformation.license_type = combinedinformation.license_type
#                     dataenrichmentinformation.file_number = combinedinformation.file_number
#                     dataenrichmentinformation.lic_or_app = combinedinformation.lic_or_app
#                     dataenrichmentinformation.type_status = combinedinformation.type_status
#                     dataenrichmentinformation.type_orig_iss_date = combinedinformation.type_orig_iss_date
#                     dataenrichmentinformation.expir_date = combinedinformation.expir_date
#                     dataenrichmentinformation.fee_codes = combinedinformation.fee_codes
#                     dataenrichmentinformation.dup_counts = combinedinformation.dup_counts
#                     dataenrichmentinformation.master_ind = combinedinformation.master_ind
#                     dataenrichmentinformation.term_in_number_of_months = combinedinformation.term_in_number_of_months
#                     dataenrichmentinformation.geo_code = combinedinformation.geo_code
#                     dataenrichmentinformation.district = combinedinformation.district
#                     dataenrichmentinformation.primary_name = combinedinformation.primary_name
#                     dataenrichmentinformation.prem_addr_1 = combinedinformation.prem_addr_1
#                     dataenrichmentinformation.prem_addr_2 = combinedinformation.prem_addr_2
#                     dataenrichmentinformation.prem_city = combinedinformation.prem_city
#                     dataenrichmentinformation.prem_state = combinedinformation.prem_state
#                     dataenrichmentinformation.prem_zip = combinedinformation.prem_zip
#                     dataenrichmentinformation.dba_name = combinedinformation.dba_name
#                     dataenrichmentinformation.mail_addr_1 = combinedinformation.mail_addr_1
#                     dataenrichmentinformation.mail_addr_2 = combinedinformation.mail_addr_2
#                     dataenrichmentinformation.mail_city = combinedinformation.mail_city
#                     dataenrichmentinformation.mail_state = combinedinformation.mail_state
#                     dataenrichmentinformation.mail_zip = combinedinformation.mail_zip
#                     dataenrichmentinformation.prem_county = combinedinformation.prem_county
#                     dataenrichmentinformation.prem_census_tract = combinedinformation.prem_census_tract
#                     dataenrichmentinformation.initial_filing_date = combinedinformation.initial_filing_date
#                     dataenrichmentinformation.jurisdiction = combinedinformation.jurisdiction
#                     dataenrichmentinformation.entity_status = combinedinformation.entity_status
#                     dataenrichmentinformation.standing_sos = combinedinformation.standing_sos
#                     dataenrichmentinformation.entity_type = combinedinformation.entity_type
#                     dataenrichmentinformation.filing_type = combinedinformation.filing_type
#                     dataenrichmentinformation.foreign_name = combinedinformation.foreign_name
#                     dataenrichmentinformation.standing_ftb = combinedinformation.standing_ftb
#                     dataenrichmentinformation.standing_vcfcf = combinedinformation.standing_vcfcf
#                     dataenrichmentinformation.standing_agent = combinedinformation.standing_agent
#                     dataenrichmentinformation.suspension_date = combinedinformation.suspension_date
#                     dataenrichmentinformation.last_si_file_number = combinedinformation.last_si_file_number
#                     dataenrichmentinformation.last_si_file_date = combinedinformation.last_si_file_date
#                     dataenrichmentinformation.llc_management_structure = combinedinformation.llc_management_structure
#                     dataenrichmentinformation.type_of_business = combinedinformation.type_of_business
#                     dataenrichmentinformation.address1 = combinedinformation.address1
#                     dataenrichmentinformation.address2 = combinedinformation.address2
#                     dataenrichmentinformation.address3 = combinedinformation.address3
#                     dataenrichmentinformation.city = combinedinformation.city
#                     dataenrichmentinformation.state = combinedinformation.state
#                     dataenrichmentinformation.country = combinedinformation.country
#                     dataenrichmentinformation.position_1 = combinedinformation.position_1
#                     dataenrichmentinformation.position_2 = combinedinformation.position_2
#                     dataenrichmentinformation.position_3 = combinedinformation.position_3
#                     dataenrichmentinformation.position_4 = combinedinformation.position_4
#                     dataenrichmentinformation.position_5 = combinedinformation.position_5
#                     dataenrichmentinformation.position_6 = combinedinformation.position_6
#                     dataenrichmentinformation.position_7 = combinedinformation.position_7
#                     dataenrichmentinformation.postal_code = combinedinformation.postal_code
#                     dataenrichmentinformation.combine_information_filed_status = True   
#                 dataenrichmentinformation.primary_owner = singledata.primary_owner    
#                 dataenrichmentinformation.office_of_application = singledata.office_of_application    
#                 dataenrichmentinformation.business_name = singledata.business_name    
#                 dataenrichmentinformation.business_address = singledata.business_address    
#                 dataenrichmentinformation.county = singledata.county    
#                 dataenrichmentinformation.census_tract = singledata.census_tract    
#                 dataenrichmentinformation.licensee = singledata.licensee    
#                 dataenrichmentinformation.license_type = singledata.license_type    
#                 dataenrichmentinformation.license_type_status = singledata.license_type_status    
#                 dataenrichmentinformation.status_date = singledata.status_date    
#                 dataenrichmentinformation.original_issue_date = singledata.original_issue_date    
#                 dataenrichmentinformation.expiration_date = singledata.expiration_date    
#                 dataenrichmentinformation.master = singledata.master    
#                 dataenrichmentinformation.fee_code = singledata.fee_code    
#                 dataenrichmentinformation.transfers = singledata.transfers    
#                 dataenrichmentinformation.conditions = singledata.conditions    
#                 dataenrichmentinformation.operating_restrictions = singledata.operating_restrictions    
#                 dataenrichmentinformation.disciplinary_action = singledata.disciplinary_action    
#                 dataenrichmentinformation.disciplinary_history = singledata.disciplinary_history    
#                 dataenrichmentinformation.holds = singledata.holds    
#                 dataenrichmentinformation.escrows = singledata.escrows    
#                 dataenrichmentinformation.to_license_number = singledata.to_license_number    
#                 dataenrichmentinformation.transferred_on2 = singledata.transferred_on2    
#                 dataenrichmentinformation.business_name_secondary = singledata.business_name_secondary    
#                 dataenrichmentinformation.business_address_secondary = singledata.business_address_secondary    
#                 dataenrichmentinformation.place_name = singledata.place_name       
#                 dataenrichmentinformation.phone_number = singledata.phone_number    
#                 dataenrichmentinformation.website = singledata.website    
#                 dataenrichmentinformation.types = singledata.types    
#                 dataenrichmentinformation.business_status = singledata.business_status    
#                 dataenrichmentinformation.primary_name = singledata.primary_name    
#                 dataenrichmentinformation.prem_addr_1 = singledata.prem_addr_1    
#                 dataenrichmentinformation.prem_addr_2 = singledata.prem_addr_2    
#                 dataenrichmentinformation.prem_city = singledata.prem_city    
#                 dataenrichmentinformation.prem_state = singledata.prem_state    
#                 dataenrichmentinformation.prem_zip = singledata.prem_zip    
#                 dataenrichmentinformation.yelp_link = singledata.yelp_link    
#                 dataenrichmentinformation.yelp_phone = singledata.yelp_phone    
#                 dataenrichmentinformation.yelp_website = singledata.yelp_website    
#                 dataenrichmentinformation.yelp_rating = singledata.yelp_rating    
#                 dataenrichmentinformation.business_License_field_status = True    
#                 dataenrichmentinformation.save()
#                 action =  "Created"
#                 logger.info(f"{full_function_name}: {action} CombinedInformation for entity_num: {dataenrichmentinformation.entity_num}")
#             logger.info(f"{full_function_name}: {message}")
#             self.message_user(request, message, messages.SUCCESS)
#             return HttpResponseRedirect("/admin/core_app/dataenrichment/")  # This
#         return merge_view
