import csv
from datetime import datetime, date
import json  # Ensure correct imports
from django.contrib import admin
from django.urls import path
from core_app.models import LicenseOutput, AbcBizYelpRestaurantData, LicenseNumber
from merge_data.models import BusinessLicense,CombinedInformation
from django.contrib import messages
  # Import BusinessLicense from app2
from django.template.response import TemplateResponse
from core_app.models import AgentsInformation, FilingsInformation, LicenseeNameDataEnrichment,PrincipalsInformation

# AbcBizYelpRestaurantData.objects.all().delete()
def parse_date(value):
    """
    Safely parse a date string into a datetime.date object.
    Returns None if the value is not a valid ISO 8601 string or not a string.
    """
    if isinstance(value, datetime):  # If it's a datetime object, convert it to date
        return value.date()
    if isinstance(value, str):  # Ensure it's a string
        value = value.strip()  # Remove leading/trailing spaces
        if not value:  # Check if the string is empty after stripping
            return None
        try:
            # Try to parse the string as an ISO 8601 date (yyyy-mm-dd)
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:  # Handle invalid date formats
            print(f"Invalid date format: {value}")
            return None
    else:
        print(f"Invalid type for date: {type(value)} - {value}")
        return None

def safe_parse_date(date_str):
    # Check if the date string is valid
    if date_str:
        try:
            # Assuming date_str is in the format "%d-%b-%y", e.g., "01-Jan-21"
            return datetime.strptime(date_str, "%d-%b-%y").strftime("%Y-%m-%d")
        except ValueError:
            return None  # Or use a default value if the date format is incorrect
    return None  # Return None if the date_str is None or empty    

@admin.register(BusinessLicense)
class BusinessLicenseAdmin(admin.ModelAdmin):
    actions = ["merge_and_import_data_action"]
    list_display = (
        "license_number","primary_owner","office_of_application",  "business_status", "county","license_type","license_type_status","term","fee_code","conditions",
        # "operating_restrictions",
        "disciplinary_action","place_name","phone_number", "rating","files_name","primary_name","dba_name","prem_addr_1","yelp_link","yelp_name","yelp_website","prem_city" )  

    def merge_data(self, request):
        # Step 1: Process data from LicenseOutput
        license_number_data = LicenseNumber.objects.all()
        for data in license_number_data:
            license_output = LicenseOutput.objects.filter(license_number=data.id).first()
            yelp_output = AbcBizYelpRestaurantData.objects.filter(file_number=data.id).first()

            try:
                # Try to get an existing BusinessLicense
                business_license = BusinessLicense.objects.get(license_number=str(data.license_number))
                found = True
            except BusinessLicense.DoesNotExist:
                # If not found, create a new one
                business_license = BusinessLicense(license_number=str(data.license_number))
                found = False

            # Update or set fields for BusinessLicense from LicenseOutput
            if license_output:
                print('license_output ke pass chal gya h')
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

            # Update or set fields for BusinessLicense from YelpOutput
            if yelp_output:
                print('yelp_output ke pass chal gya h')
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
                business_license.yelp_file_status = True
            # Save the business_license object after updating/creating
            business_license.save()

            action = "Updated" if found else "Created"
            print(f"{action} BusinessLicense for license_number: {business_license.license_number}")
            print(f"{action} BusinessLicense for Yelp data: {yelp_output}")

        self.message_user(request, "Data imported successfully!", messages.SUCCESS)

        # Prepare context for the template
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
        }

        # Return the template response
        return TemplateResponse(request, "admin/merge_form.html", context)
      
    merge_data.short_description = "merge data"

    def get_urls(self):
        """
        Add a custom URL for CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("merge_data/", self.merge_data, name="merge_data"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['merge_data_url'] = 'admin:merge_data'
        return super().changelist_view(request, extra_context=extra_context)


@admin.register(CombinedInformation)
class CombinedInformationAdmin(admin.ModelAdmin):
    actions = ["merge_and_import_data_action"]
    list_display = (
        "entity_name","entity_num","first_name","license_type",)  

    def merge_data(self, request):
        licensee_enrichment_data = PrincipalsInformation.objects.all()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
        }

        
        
        for singledata in licensee_enrichment_data:
            try:
                # Try to get an existing BusinessLicense
                combined_information = CombinedInformation.objects.get(entity_num=str(singledata.entity_num))
                found = True
            except CombinedInformation.DoesNotExist:
                # If not found, create a new one
                combined_information = CombinedInformation(entity_num=str(singledata.entity_num))
                found = False
            print('data', singledata.entity_num)
            agent_informations = AgentsInformation.objects.filter(entity_num=singledata.entity_num).first()
            filings_information = FilingsInformation.objects.filter(entity_num=singledata.entity_num).first()
            print('agent_informations', agent_informations)
            print('filings_information', agent_informations)
            if agent_informations:
                print('agent_informations s aa gya h ')
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
                print('filings_information s aa gya h ') 
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
            action = "Updated" if found else "Created"
            # break
        self.message_user(request, "Data imported successfully!", messages.SUCCESS)
        # Prepare context for the template
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
        }

        # Return the template response
        return TemplateResponse(request, "admin/merge_form.html", context)
    
    merge_data.short_description = "merge data"

    def get_urls(self):
        """
        Add a custom URL for CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("informations_data/", self.merge_data, name="informations_data"),
        ]
        return custom_urls + urls

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['informations_data_url'] = 'admin:informations_data'
        return super().changelist_view(request, extra_context=extra_context)



