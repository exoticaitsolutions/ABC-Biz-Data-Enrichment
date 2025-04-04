from django.contrib import admin, messages
from django.http import HttpResponseRedirect
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from core_app.models import CompanyInformationRecord
from merge_data.models import  DataErichmentWithoutConpanyInfo
from yelprecords.models import AssociatedContactMapping, LicenseeProfile, UniqueLocationLicense
from ABC_BizEnrichment.common.logconfig import logger
@admin.register(LicenseeProfile)
class LicenseeProfileAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "licenseeprofilerecords"
    list_display = ("id","abc_licensee","abc_dba_name")
    def create_or_update_licensee_profile(self, abc_license_number, mapping):
        obj, created = UniqueLocationLicense.objects.update_or_create( abc_license_number=abc_license_number,  defaults=mapping)
        return obj, created
    # search_fields = ['entity_name', 'licensee']  # Add more fields here if needed
    def get_merge_view(self):
        def DataSet3Recordmerge_view(request):
            full_function_name = get_full_function_name()
            batch_size = 10
            dataerichment = DataErichmentWithoutConpanyInfo.objects.all()
            total_dataerichment_count = dataerichment.count()
            records_to_create = []
            for i in tqdm(range(0, total_dataerichment_count, batch_size), desc="Processing Data", unit="batch"):
                betchdataerichment = dataerichment[i:i + batch_size]
                for batch in betchdataerichment:
                    abc_license_number = str(batch.abc_license_number) 
                    if abc_license_number:
                        matching_second_records = CompanyInformationRecord.objects.filter(Company_Info_License_Number=abc_license_number)
                        if matching_second_records:
                            Company_Info_License_Number = matching_second_records[0].Company_Info_License_Number
                        else:
                            Company_Info_License_Number = ''
                        LicenseeProfilemapping = {
                        'Company_Info_License_Number': Company_Info_License_Number,
                        'abc_licensee': batch.abc_licensee,
                        'abc_primary_name': batch.abc_primary_name,
                        'abc_dba_name':batch.abc_dba_name,
                        'abc_mail_addr_1':batch.abc_mail_addr_1,
                        'abc_mail_city':batch.abc_mail_city,
                        'abc_mail_state':batch.abc_mail_state,
                        'abc_mail_zip':batch.abc_mail_zip,
                        'google_phone_number':batch.google_phone_number,
                        'google_website':batch.google_website,
                        'yelp_phone':batch.yelp_phone,
                        'yelp_web_site':batch.yelp_web_site,
                        'agentsInformation_entity_name':batch.agentsInformation_entity_name,
                        'agentsInformation_entity_num':batch.agentsInformation_entity_num,
                        'agentsInformation_org_name':batch.agentsInformation_org_name,
                        'filingsInformation_entity_num': batch.filingsInformation_entity_num,
                        'filingsInformation_entity_name':batch.filingsInformation_entity_name,
                        'filingsInformation_initial_filing_date':batch.filingsInformation_initial_filing_date,
                        'filingsInformation_jurisdiction':batch.filingsInformation_jurisdiction,
                        'filingsInformation_entity_status':batch.filingsInformation_entity_status,
                        'filingsInformation_standing_sos':batch.filingsInformation_standing_sos,
                        'filingsInformation_entity_type':batch.filingsInformation_entity_type,
                        'filingsInformation_filing_type':batch.filingsInformation_filing_type,
                        'filingsInformation_foreign_name':batch.filingsInformation_foreign_name,
                        'filingsInformation_standing_ftb':batch.filingsInformation_standing_ftb,
                        'filingsInformation_standing_vcfcf':batch.filingsInformation_standing_vcfcf,
                        'filingsInformation_standing_agent':batch.filingsInformation_standing_agent,
                        'filingsInformation_suspension_date':batch.filingsInformation_suspension_date,
                        'filingsInformation_last_si_file_number':batch.filingsInformation_last_si_file_number,
                        'filingsInformation_last_si_file_date':batch.filingsInformation_last_si_file_date,
                        'filingsInformation_principal_address':batch.filingsInformation_principal_address,
                        'filingsInformation_principal_address2':batch.filingsInformation_principal_address2,
                        'filingsInformation_principal_city':batch.filingsInformation_principal_city,
                        'filingsInformation_principal_state':batch.filingsInformation_principal_state,
                        'filingsInformation_principal_country':batch.filingsInformation_principal_country,
                        'filingsInformation_principal_postal_code':batch.filingsInformation_principal_postal_code,
                        'filingsInformation_mailing_address':batch.filingsInformation_mailing_address,
                        'filingsInformation_mailing_address2':batch.filingsInformation_mailing_address2,
                        'filingsInformation_mailing_address3':batch.filingsInformation_mailing_address3,
                        'filingsInformation_mailing_city':batch.filingsInformation_mailing_city,
                        'filingsInformation_mailing_state':batch.filingsInformation_mailing_state,
                        'filingsInformation_mailing_country':batch.filingsInformation_mailing_country,
                        'filingsInformation_mailing_postal_code':batch.filingsInformation_mailing_postal_code,
                        'filingsInformation_principal_address_in_ca':batch.filingsInformation_principal_address_in_ca,
                        'filingsInformation_principal_address2_in_ca':batch.filingsInformation_principal_address2_in_ca,
                        'filingsInformation_principal_city_in_ca':batch.filingsInformation_principal_city_in_ca,
                        'filingsInformation_principal_state_in_ca':batch.filingsInformation_principal_state_in_ca,
                        'filingsInformation_principal_country_in_ca':batch.filingsInformation_principal_country_in_ca,
                        'filingsInformation_principal_postal_code_in_ca':batch.filingsInformation_principal_postal_code_in_ca,
                        'filingsInformation_llc_management_structure':batch.filingsInformation_llc_management_structure,
                        'filingsInformation_type_of_business':batch.filingsInformation_type_of_business,
                        'principalsInformation_entity_name':batch.principalsInformation_entity_name,
                        'principalsInformation_entity_num':batch.principalsInformation_entity_num
                        }
                        obj, created = UniqueLocationLicense.objects.update_or_create( abc_license_number=abc_license_number,  defaults=LicenseeProfilemapping)
                        new_LicenseeProfile = LicenseeProfile()
                        new_LicenseeProfile.abc_license_number = abc_license_number
                        new_LicenseeProfile.Company_Info_License_Number = Company_Info_License_Number
                        new_LicenseeProfile.abc_business_address = batch.abc_business_address
                        new_LicenseeProfile.abc_county = batch.abc_county
                        new_LicenseeProfile.abc_census_tract = batch.abc_census_tract
                        new_LicenseeProfile.abc_licensee = batch.abc_licensee
                        new_LicenseeProfile.abc_license_type = batch.abc_license_type
                        new_LicenseeProfile.abc_license_type_status = batch.abc_license_type_status
                        new_LicenseeProfile.abc_status_date = batch.abc_status_date
                        new_LicenseeProfile.abc_term = batch.abc_term
                        new_LicenseeProfile.abc_original_issue_date = batch.abc_original_issue_date
                        new_LicenseeProfile.abc_expiration_date = batch.abc_expiration_date
                        new_LicenseeProfile.abc_master = batch.abc_master
                        new_LicenseeProfile.abc_duplicate = batch.abc_duplicate
                        new_LicenseeProfile.abc_fee_code = batch.abc_fee_code
                        new_LicenseeProfile.abc_transfers = batch.abc_transfers
                        new_LicenseeProfile.abc_conditions = batch.abc_conditions
                        new_LicenseeProfile.abc_operating_restrictions = batch.abc_operating_restrictions
                        new_LicenseeProfile.abc_disciplinary_action = batch.abc_disciplinary_action
                        new_LicenseeProfile.abc_disciplinary_history = batch.abc_disciplinary_history
                        new_LicenseeProfile.abc_holds = batch.abc_holds
                        new_LicenseeProfile.abc_escrows = batch.abc_escrows
                        new_LicenseeProfile.abc_from_license_number = batch.abc_from_license_number
                        new_LicenseeProfile.abc_transferred_on = batch.abc_transferred_on
                        new_LicenseeProfile.abc_to_license_number = batch.abc_to_license_number
                        new_LicenseeProfile.abc_transferred_on2 = batch.abc_transferred_on2
                        new_LicenseeProfile.abc_license_type = batch.abc_license_type
                        new_LicenseeProfile.abc_file_number = batch.abc_file_number
                        new_LicenseeProfile.abc_lic_or_app = batch.abc_lic_or_app
                        new_LicenseeProfile.abc_type_status = batch.abc_type_status
                        new_LicenseeProfile.abc_type_orig_iss_date = batch.abc_type_orig_iss_date
                        new_LicenseeProfile.abc_expir_date = batch.abc_expir_date
                        new_LicenseeProfile.abc_fee_codes = batch.abc_fee_codes
                        new_LicenseeProfile.abc_dup_counts = batch.abc_dup_counts
                        new_LicenseeProfile.abc_master_ind = batch.abc_master_ind
                        new_LicenseeProfile.abc_term_in_number_of_months = batch.abc_term_in_number_of_months
                        new_LicenseeProfile.abc_geo_code = batch.abc_geo_code
                        new_LicenseeProfile.abc_district = batch.abc_district
                        new_LicenseeProfile.abc_primary_name = batch.abc_primary_name
                        new_LicenseeProfile.abc_prem_addr_1 = batch.abc_prem_addr_1
                        new_LicenseeProfile.abc_prem_addr_2 = batch.abc_prem_addr_2
                        new_LicenseeProfile.abc_prem_city = batch.abc_prem_city
                        new_LicenseeProfile.abc_prem_state = batch.abc_prem_state
                        new_LicenseeProfile.abc_prem_zip = batch.abc_prem_zip
                        new_LicenseeProfile.abc_dba_name = batch.abc_dba_name
                        new_LicenseeProfile.abc_prem_county = batch.abc_prem_county
                        new_LicenseeProfile.google_business_name = batch.google_business_name
                        new_LicenseeProfile.google_business_address = batch.google_business_address
                        new_LicenseeProfile.google_place_name = batch.google_place_name
                        new_LicenseeProfile.google_rating = batch.google_rating
                        new_LicenseeProfile.google_phone_number = batch.google_phone_number
                        new_LicenseeProfile.google_website = batch.google_website
                        new_LicenseeProfile.google_types = batch.google_types
                        new_LicenseeProfile.google_business_status = batch.google_business_status
                        new_LicenseeProfile.yelp_file_number = batch.yelp_file_number
                        new_LicenseeProfile.yelp_license_type = batch.yelp_license_type
                        new_LicenseeProfile.yelp_primary_name = batch.yelp_primary_name
                        new_LicenseeProfile.yelp_dba_name = batch.yelp_dba_name
                        new_LicenseeProfile.yelp_prem_addr_1 = batch.yelp_prem_addr_1
                        new_LicenseeProfile.yelp_prem_addr_2 = batch.yelp_prem_addr_2
                        new_LicenseeProfile.yelp_prem_city = batch.yelp_prem_city
                        new_LicenseeProfile.yelp_prem_state = batch.yelp_prem_state
                        new_LicenseeProfile.yelp_prem_zip = batch.yelp_prem_zip
                        new_LicenseeProfile.yelp_link = batch.yelp_link
                        new_LicenseeProfile.yelp_name = batch.yelp_name
                        new_LicenseeProfile.yelp_phone = batch.yelp_phone
                        new_LicenseeProfile.yelp_web_site = batch.yelp_web_site
                        new_LicenseeProfile.yelp_rating = batch.yelp_rating
                        new_LicenseeProfile.output_license_file_status =  bool(batch.output_license_file_status)
                        new_LicenseeProfile.yelp_file_status = bool(batch.yelp_file_status)
                        new_LicenseeProfile.filingsInformation_entity_num = batch.filingsInformation_entity_num
                        new_LicenseeProfile.save()
                        if matching_second_records:
                            for rec in matching_second_records:
                                new_AssociatedContactMapping= AssociatedContactMapping()
                                new_AssociatedContactMapping.abc_license_number = abc_license_number
                                new_AssociatedContactMapping.Company_Info_License_Number = rec.Company_Info_Type
                                new_AssociatedContactMapping.Company_Info_Type = rec.Company_Info_Type
                                new_AssociatedContactMapping.Company_Info_Name = rec.Company_Info_Name
                                new_AssociatedContactMapping.Company_Info_Role = rec.Company_Info_Role
                                new_AssociatedContactMapping.agentsInformation_entity_num = batch.agentsInformation_entity_num
                                new_AssociatedContactMapping.agentsInformation_org_name = batch.agentsInformation_org_name
                                new_AssociatedContactMapping.agentsInformation_first_name = batch.agentsInformation_first_name
                                new_AssociatedContactMapping.agentsInformation_middle_name = batch.agentsInformation_middle_name
                                new_AssociatedContactMapping.agentsInformation_last_name = batch.agentsInformation_last_name
                                new_AssociatedContactMapping.agentsInformation_physical_address1 = batch.agentsInformation_physical_address1
                                new_AssociatedContactMapping.agentsInformation_physical_address2 = batch.agentsInformation_physical_address2
                                new_AssociatedContactMapping.agentsInformation_physical_address3 = batch.agentsInformation_physical_address3
                                new_AssociatedContactMapping.agentsInformation_physical_city = batch.agentsInformation_physical_city
                                new_AssociatedContactMapping.agentsInformation_physical_state = batch.agentsInformation_physical_state
                                new_AssociatedContactMapping.agentsInformation_physical_country = batch.agentsInformation_physical_country
                                new_AssociatedContactMapping.agentsInformation_physical_postal_code = batch.agentsInformation_physical_postal_code
                                new_AssociatedContactMapping.agentsInformation_agent_type = batch.agentsInformation_agent_type
                                new_AssociatedContactMapping.filingsInformation_entity_num = batch.filingsInformation_entity_num
                                new_AssociatedContactMapping.principalsInformation_entity_num = batch.principalsInformation_entity_num
                                new_AssociatedContactMapping.principalsInformation_org_name = batch.principalsInformation_org_name
                                new_AssociatedContactMapping.principalsInformation_first_name = batch.principalsInformation_first_name
                                new_AssociatedContactMapping.principalsInformation_middle_name = batch.principalsInformation_middle_name
                                new_AssociatedContactMapping.principalsInformation_last_name = batch.principalsInformation_last_name
                                new_AssociatedContactMapping.principalsInformation_address1 = batch.principalsInformation_address1
                                new_AssociatedContactMapping.principalsInformation_address2 = batch.principalsInformation_address2
                                new_AssociatedContactMapping.principalsInformation_address3 = batch.principalsInformation_address3
                                new_AssociatedContactMapping.principalsInformation_city = batch.principalsInformation_city
                                new_AssociatedContactMapping.principalsInformation_state = batch.principalsInformation_state
                                new_AssociatedContactMapping.principalsInformation_country = batch.principalsInformation_country
                                new_AssociatedContactMapping.principalsInformation_postal_code = batch.principalsInformation_postal_code
                                new_AssociatedContactMapping.principalsInformation_position_1 = batch.principalsInformation_position_1
                                new_AssociatedContactMapping.principalsInformation_position_2 = batch.principalsInformation_position_2
                                new_AssociatedContactMapping.principalsInformation_position_3 = batch.principalsInformation_position_3
                                new_AssociatedContactMapping.principalsInformation_position_4 = batch.principalsInformation_position_4
                                new_AssociatedContactMapping.principalsInformation_position_5 = batch.principalsInformation_position_5
                                new_AssociatedContactMapping.principalsInformation_position_6 = batch.principalsInformation_position_6
                                new_AssociatedContactMapping.principalsInformation_position_7 = batch.principalsInformation_position_7
                                new_AssociatedContactMapping.filling_information_file_status = bool(batch.filling_information_file_status)
                                new_AssociatedContactMapping.principal_information_file_status = bool(batch.principal_information_file_status)
                                new_AssociatedContactMapping.agentsInformation_file_status = bool(batch.agentsInformation_file_status)
                                new_AssociatedContactMapping.data_set_1_file_status = bool(batch.data_set_1_file_status)
                                new_AssociatedContactMapping.data_set_2_file_status = bool(batch.data_set_2_file_status)
                                new_AssociatedContactMapping.save()
                    # break
                # break
            message = 'Data Sync Process Successfully'
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/yelprecords/licenseeprofile/")
        return DataSet3Recordmerge_view     