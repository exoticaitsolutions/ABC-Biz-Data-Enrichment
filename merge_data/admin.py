from datetime import datetime
import logging
from django.db.models import Q
import time
from django.contrib import admin
from django.template.response import TemplateResponse
from core_app.models import AbcBizYelpRestaurantData, AgentsInformation, FilingsInformation, LicenseOutput, PrincipalsInformation
from merge_data.models import BusinessLicense, CombinedInformation, DataEnrichment
from django.contrib import messages
from ABC_BizEnrichment.common.helper_function import get_full_function_name, get_model_field_names, safe_parse_date
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.urls import path
# Logger setup
logger = logging.getLogger(__name__)
BATCH_SIZE = 10000  # Number of rows to import at a time
@admin.register(BusinessLicense)
class BusinessLicenseAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    actions = ["merge_and_import_data_action"]
    list_display = ("license_number", "primary_name", "yelp_name", "yelp_website", "prem_city", "yelp_file_status")
    merge_url_name = "merge_data"
    
    def get_merge_view(self):
        def merge_view(request):
            full_function_name = get_full_function_name()
            batch_size = 10000  # Number of records to process at once
            license_output_qs = LicenseOutput.objects.all()
            total_license_count = license_output_qs.count()
            for i in range(0, total_license_count, batch_size):
                batch = license_output_qs[i:i + batch_size]  # Fetch the current batch
                for license_output in batch:
                    yelp_output = AbcBizYelpRestaurantData.objects.filter(file_number=license_output.license_number).first()
                    business_license, created = BusinessLicense.objects.get_or_create(license_number=str(license_output.license_number))
                    if yelp_output:
                        business_license.primary_name = yelp_output.primary_name
                        business_license.dba_name = yelp_output.dba_name
                        business_license.prem_addr_1 = yelp_output.prem_addr_1
                        business_license.prem_addr_2 = yelp_output.prem_addr_2
                        business_license.prem_city = yelp_output.prem_city
                        business_license.prem_state = yelp_output.prem_state
                        business_license.prem_zip = yelp_output.prem_zip
                        business_license.yelp_link = yelp_output.yelp_link
                        business_license.yelp_name = yelp_output.yelp_name
                        business_license.yelp_phone = yelp_output.yelp_phone
                        business_license.yelp_website = yelp_output.yelp_web_site
                        business_license.yelp_rating = yelp_output.yelp_rating
                        business_license.yelp_file_status = True
                        business_license.primary_owner = license_output.primary_owner
                        business_license.office_of_application = license_output.office_of_application
                        business_license.business_name = license_output.business_name
                        business_license.business_address = license_output.business_address
                        business_license.county = license_output.county
                        business_license.census_tract = license_output.census_tract
                        business_license.license_type = license_output.license_type
                        business_license.license_type_status = license_output.license_type_status
                        business_license.status_date = safe_parse_date(license_output.status_date)
                        business_license.original_issue_date = safe_parse_date(license_output.original_issue_date)
                        business_license.expiration_date = safe_parse_date(license_output.expiration_date)
                        business_license.master = license_output.master
                        business_license.fee_code = license_output.fee_code
                        business_license.licensee = license_output.licensee
                        business_license.transfers = True if license_output.transfers else False
                        business_license.conditions = license_output.conditions
                        business_license.operating_restrictions = license_output.operating_restrictions
                        business_license.disciplinary_action = license_output.disciplinary_action
                        business_license.disciplinary_history = license_output.disciplinary_history
                        business_license.holds = license_output.holds
                        business_license.escrows = license_output.escrows
                        business_license.from_license_number = license_output.from_license_number
                        business_license.to_license_number = license_output.to_license_number
                        business_license.business_name_secondary = license_output.business_name_alt
                        business_license.business_address_secondary = license_output.business_address_alt
                        business_license.place_name = license_output.place_name
                        business_license.phone_number = license_output.phone_number
                        business_license.website = license_output.website
                        business_license.types = license_output.types
                        business_license.business_status = license_output.business_status
                        business_license.output_lincense_file_status = True
                        business_license.save()
                    action = "Updated" if not created else "Created"
                    logger.info(f"{full_function_name}: {action} BusinessLicense for license_number: {business_license.license_number}")
                # Optional: log batch progress
                logger.info(f"Processed batch {i // batch_size + 1} of {total_license_count // batch_size + 1}.")
            logger.info(f"{full_function_name}: Data imported successfully!")
            self.message_user(request, "Data imported successfully!", messages.SUCCESS)
            return TemplateResponse(request, "admin/merge_form.html", {"opts": self.model._meta})
        return merge_view
    
@admin.register(CombinedInformation)
class CombinedInformationAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "informations_data"
    list_display = ("entity_name", "entity_num", "first_name", "license_type")
    search_fields = ['entity_name']  # You can add other fields here as needed
    def get_merge_view(self):
        def merge_view(request):
            # licensee_enrichment_data = PrincipalsInformation.objects.all()
            licensee_enrichment_data = PrincipalsInformation.objects.all()
            total_license_count = licensee_enrichment_data.count()
            full_function_name = get_full_function_name()
            logger.info(f"{full_function_name}: totat number of the PrincipalsInformation data: {total_license_count}")
            for singledata in licensee_enrichment_data:
                combined_information = CombinedInformation()
                agent_informations = AgentsInformation.objects.filter(entity_num=str(singledata.entity_num)).first()
                print('agent_informations', agent_informations)
                filings_information = FilingsInformation.objects.filter(entity_num=str(singledata.entity_num)).first()
                print('filings_information', filings_information)
                if agent_informations:
                    combined_information.entity_num = agent_informations.entity_num
                    combined_information.entity_name = agent_informations.entity_name
                    combined_information.org_name = agent_informations.org_name
                    combined_information.first_name = agent_informations.first_name
                    combined_information.middle_name = agent_informations.middle_name
                    combined_information.last_name = agent_informations.last_name
                    combined_information.physical_address1 = agent_informations.physical_address1
                    combined_information.physical_address2 = agent_informations.physical_address2
                    combined_information.physical_address3 = agent_informations.physical_address3
                    combined_information.physical_city = agent_informations.physical_city
                    combined_information.physical_state = agent_informations.physical_state
                    combined_information.physical_country = agent_informations.physical_country
                    combined_information.physical_postal_code = agent_informations.physical_postal_code
                    combined_information.agent_type = agent_informations.agent_type
                    combined_information.agent_file_status = True
                if filings_information: 
                    combined_information.entity_num = filings_information.entity_num
                    combined_information.license_type = filings_information.license_type
                    combined_information.file_number = filings_information.file_number
                    combined_information.lic_or_app = filings_information.lic_or_app
                    combined_information.type_status = filings_information.type_status
                    combined_information.type_orig_iss_date  = filings_information.type_orig_iss_date
                    combined_information.expir_date = filings_information.expir_date
                    combined_information.fee_codes = filings_information.fee_codes
                    combined_information.dup_counts = filings_information.dup_counts
                    combined_information.master_ind = filings_information.master_ind
                    combined_information.term_in_number_of_months = filings_information.term_in_number_of_months
                    combined_information.geo_code = filings_information.geo_code
                    combined_information.district = filings_information.district
                    combined_information.primary_name = filings_information.primary_name
                    combined_information.prem_addr_1 = filings_information.prem_addr_1
                    combined_information.prem_addr_2 = filings_information.prem_addr_2
                    combined_information.prem_city = filings_information.prem_city
                    combined_information.prem_state = filings_information.prem_state
                    combined_information.prem_zip = filings_information.prem_zip
                    combined_information.dba_name = filings_information.dba_name
                    combined_information.mail_addr_1 = filings_information.mail_addr_1
                    combined_information.mail_addr_2 = filings_information.mail_addr_2
                    combined_information.mail_city = filings_information.mail_city
                    combined_information.mail_state = filings_information.mail_state
                    combined_information.mail_zip = filings_information.mail_zip
                    combined_information.prem_county = filings_information.prem_county
                    combined_information.prem_census_tract = filings_information.prem_census_tract
                    combined_information.filling_file_status = True
                combined_information.entity_num = singledata.entity_num
                combined_information.entity_name = singledata.entity_name
                combined_information.org_name = singledata.org_name
                combined_information.first_name = singledata.first_name
                combined_information.last_name = singledata.last_name
                combined_information.address1 = singledata.address1
                combined_information.address2 = singledata.address2
                combined_information.address3 = singledata.address3
                combined_information.city = singledata.city
                combined_information.state = singledata.state
                combined_information.country = singledata.country
                combined_information.postal_code = singledata.postal_code
                combined_information.position_1 = singledata.position_1
                combined_information.position_2 = singledata.position_2
                combined_information.position_3 = singledata.position_3
                combined_information.position_4 = singledata.position_4
                combined_information.position_5 = singledata.position_5
                combined_information.position_6 = singledata.position_6
                combined_information.position_7 = singledata.position_7
                combined_information.principal_file_status = True
                combined_information.save()
                action =  "Created"
                logger.info(f"{full_function_name}: {action} CombinedInformation for entity_num: {combined_information.entity_num}")
            logger.info(f"{full_function_name}: Data imported successfully!")
            self.message_user(request, "Data imported successfully!", messages.SUCCESS)
            return TemplateResponse(request, "admin/merge_form.html", {"opts": self.model._meta})
        return merge_view

@admin.register(DataEnrichment)
class DataEnrichmentAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    actions = ["merge_and_import_data_action"]
    search_fields = ['business_name', 'license_number', 'city']  # Use valid field names
    list_display = ("license_number","primary_owner", "business_name","business_License_field_status",
    "combine_information_filed_status")
    
    merge_url_name = "enrichment_data"
    def get_merge_view(self):
        """
        Subclass-specific view for handling the merge logic.
        """
        def merge_view(request):
            businesslicense_data = BusinessLicense.objects.all()
            total_license_count = businesslicense_data.count()
            full_function_name = get_full_function_name()

            logger.info(f"{full_function_name}: totat number of the BusinessLicense data: {total_license_count}")
            for singledata in businesslicense_data:
                dataenrichmentinformation = DataEnrichment()
                print('licensee', singledata.licensee)
                combinedinformation = CombinedInformation.objects.filter(entity_name__icontains=str(singledata.licensee)).first()
                print('combinedinformation', combinedinformation)
                if combinedinformation:
                    dataenrichmentinformation.entity_num = combinedinformation.entity_num
                    dataenrichmentinformation.org_name = combinedinformation.org_name
                    dataenrichmentinformation.first_name = combinedinformation.first_name
                    dataenrichmentinformation.middle_name = combinedinformation.middle_name
                    dataenrichmentinformation.last_name = combinedinformation.last_name
                    dataenrichmentinformation.physical_address1 = combinedinformation.physical_address1
                    dataenrichmentinformation.physical_address2 = combinedinformation.physical_address2
                    dataenrichmentinformation.physical_address3 = combinedinformation.physical_address3
                    dataenrichmentinformation.physical_city = combinedinformation.physical_city
                    dataenrichmentinformation.physical_state = combinedinformation.physical_state
                    dataenrichmentinformation.physical_country = combinedinformation.physical_country
                    dataenrichmentinformation.physical_postal_code = combinedinformation.physical_postal_code
                    dataenrichmentinformation.agent_type = combinedinformation.agent_type
                    dataenrichmentinformation.license_type = combinedinformation.license_type
                    dataenrichmentinformation.file_number = combinedinformation.file_number
                    dataenrichmentinformation.lic_or_app = combinedinformation.lic_or_app
                    dataenrichmentinformation.type_status = combinedinformation.type_status
                    dataenrichmentinformation.type_orig_iss_date = combinedinformation.type_orig_iss_date
                    dataenrichmentinformation.expir_date = combinedinformation.expir_date
                    dataenrichmentinformation.fee_codes = combinedinformation.fee_codes
                    dataenrichmentinformation.dup_counts = combinedinformation.dup_counts
                    dataenrichmentinformation.master_ind = combinedinformation.master_ind
                    dataenrichmentinformation.term_in_number_of_months = combinedinformation.term_in_number_of_months
                    dataenrichmentinformation.geo_code = combinedinformation.geo_code
                    dataenrichmentinformation.district = combinedinformation.district
                    dataenrichmentinformation.primary_name = combinedinformation.primary_name
                    dataenrichmentinformation.prem_addr_1 = combinedinformation.prem_addr_1
                    dataenrichmentinformation.prem_addr_2 = combinedinformation.prem_addr_2
                    dataenrichmentinformation.prem_city = combinedinformation.prem_city
                    dataenrichmentinformation.prem_state = combinedinformation.prem_state
                    dataenrichmentinformation.prem_zip = combinedinformation.prem_zip
                    dataenrichmentinformation.dba_name = combinedinformation.dba_name
                    dataenrichmentinformation.mail_addr_1 = combinedinformation.mail_addr_1
                    dataenrichmentinformation.mail_addr_2 = combinedinformation.mail_addr_2
                    dataenrichmentinformation.mail_city = combinedinformation.mail_city
                    dataenrichmentinformation.mail_state = combinedinformation.mail_state
                    dataenrichmentinformation.mail_zip = combinedinformation.mail_zip
                    dataenrichmentinformation.prem_county = combinedinformation.prem_county
                    dataenrichmentinformation.prem_census_tract = combinedinformation.prem_census_tract
                    dataenrichmentinformation.initial_filing_date = combinedinformation.initial_filing_date
                    dataenrichmentinformation.jurisdiction = combinedinformation.jurisdiction
                    dataenrichmentinformation.entity_status = combinedinformation.entity_status
                    dataenrichmentinformation.standing_sos = combinedinformation.standing_sos
                    dataenrichmentinformation.entity_type = combinedinformation.entity_type
                    dataenrichmentinformation.filing_type = combinedinformation.filing_type
                    dataenrichmentinformation.foreign_name = combinedinformation.foreign_name
                    dataenrichmentinformation.standing_ftb = combinedinformation.standing_ftb
                    dataenrichmentinformation.standing_vcfcf = combinedinformation.standing_vcfcf
                    dataenrichmentinformation.standing_agent = combinedinformation.standing_agent
                    dataenrichmentinformation.suspension_date = combinedinformation.suspension_date
                    dataenrichmentinformation.last_si_file_number = combinedinformation.last_si_file_number
                    dataenrichmentinformation.last_si_file_date = combinedinformation.last_si_file_date
                    dataenrichmentinformation.llc_management_structure = combinedinformation.llc_management_structure
                    dataenrichmentinformation.type_of_business = combinedinformation.type_of_business
                    dataenrichmentinformation.address1 = combinedinformation.address1
                    dataenrichmentinformation.address2 = combinedinformation.address2
                    dataenrichmentinformation.address3 = combinedinformation.address3
                    dataenrichmentinformation.city = combinedinformation.city
                    dataenrichmentinformation.state = combinedinformation.state
                    dataenrichmentinformation.country = combinedinformation.country
                    dataenrichmentinformation.position_1 = combinedinformation.position_1
                    dataenrichmentinformation.position_2 = combinedinformation.position_2
                    dataenrichmentinformation.position_3 = combinedinformation.position_3
                    dataenrichmentinformation.position_4 = combinedinformation.position_4
                    dataenrichmentinformation.position_5 = combinedinformation.position_5
                    dataenrichmentinformation.position_6 = combinedinformation.position_6
                    dataenrichmentinformation.position_7 = combinedinformation.position_7
                    dataenrichmentinformation.postal_code = combinedinformation.postal_code
                    dataenrichmentinformation.combine_information_filed_status = True
                dataenrichmentinformation.license_number = singledata.license_number    
                dataenrichmentinformation.primary_owner = singledata.primary_owner    
                dataenrichmentinformation.office_of_application = singledata.office_of_application    
                dataenrichmentinformation.business_name = singledata.business_name    
                dataenrichmentinformation.business_address = singledata.business_address    
                dataenrichmentinformation.county = singledata.county    
                dataenrichmentinformation.census_tract = singledata.census_tract    
                dataenrichmentinformation.licensee = singledata.licensee    
                dataenrichmentinformation.license_type = singledata.license_type    
                dataenrichmentinformation.license_type_status = singledata.license_type_status    
                dataenrichmentinformation.status_date = singledata.status_date    
                dataenrichmentinformation.original_issue_date = singledata.original_issue_date    
                dataenrichmentinformation.expiration_date = singledata.expiration_date    
                dataenrichmentinformation.master = singledata.master    
                dataenrichmentinformation.fee_code = singledata.fee_code    
                dataenrichmentinformation.transfers = singledata.transfers    
                dataenrichmentinformation.conditions = singledata.conditions    
                dataenrichmentinformation.operating_restrictions = singledata.operating_restrictions    
                dataenrichmentinformation.disciplinary_action = singledata.disciplinary_action    
                dataenrichmentinformation.disciplinary_history = singledata.disciplinary_history    
                dataenrichmentinformation.holds = singledata.holds    
                dataenrichmentinformation.escrows = singledata.escrows    
                dataenrichmentinformation.to_license_number = singledata.to_license_number    
                dataenrichmentinformation.transferred_on2 = singledata.transferred_on2    
                dataenrichmentinformation.business_name_secondary = singledata.business_name_secondary    
                dataenrichmentinformation.business_address_secondary = singledata.business_address_secondary    
                dataenrichmentinformation.place_name = singledata.place_name       
                dataenrichmentinformation.phone_number = singledata.phone_number    
                dataenrichmentinformation.website = singledata.website    
                dataenrichmentinformation.types = singledata.types    
                dataenrichmentinformation.business_status = singledata.business_status    
                dataenrichmentinformation.primary_name = singledata.primary_name    
                dataenrichmentinformation.prem_addr_1 = singledata.prem_addr_1    
                dataenrichmentinformation.prem_addr_2 = singledata.prem_addr_2    
                dataenrichmentinformation.prem_city = singledata.prem_city    
                dataenrichmentinformation.prem_state = singledata.prem_state    
                dataenrichmentinformation.prem_zip = singledata.prem_zip    
                dataenrichmentinformation.yelp_link = singledata.yelp_link    
                dataenrichmentinformation.yelp_phone = singledata.yelp_phone    
                dataenrichmentinformation.yelp_website = singledata.yelp_website    
                dataenrichmentinformation.yelp_rating = singledata.yelp_rating    
                dataenrichmentinformation.business_License_field_status = True    
                dataenrichmentinformation.save()
                action =  "Created"
                logger.info(f"{full_function_name}: {action} CombinedInformation for entity_num: {dataenrichmentinformation.entity_num}")
            logger.info(f"{full_function_name}: Data Merge  successfully! From DataSet 1 and DataSet2!")
            self.message_user(request, "Data Merge  successfully! From DataSet 1 and DataSet2", messages.SUCCESS)
            return super(DataEnrichmentAdmin, self).changelist_view(request)  # This fixes the `super()` call
        return merge_view