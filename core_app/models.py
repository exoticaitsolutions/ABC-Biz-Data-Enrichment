from django.db import models
# Model for Generating Data Set 1 Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
class LicenseOutput(models.Model):
    abc_license_number = models.TextField(blank=True, null=True)  # Converted to TextField
    abc_primary_owner = models.TextField( null=True, blank=True)
    abc_office_of_application = models.TextField( null=True, blank=True)
    abc_business_name = models.TextField( null=True, blank=True)
    abc_business_address = models.TextField( null=True, blank=True)
    abc_county = models.TextField( null=True, blank=True)
    abc_census_tract = models.TextField(blank=True, null=True)
    abc_licensee = models.TextField(null=True, blank=True)
    abc_license_type = models.TextField( null=True, blank=True)
    abc_license_type_status = models.TextField( null=True, blank=True)
    abc_status_date = models.DateField(null=True, blank=True)
    abc_term = models.TextField( null=True, blank=True)
    abc_original_issue_date = models.DateField(null=True, blank=True)
    abc_expiration_date = models.DateField(null=True, blank=True)
    abc_master = models.TextField( null=True, blank=True)
    abc_duplicate = models.BooleanField(default=False, null=True, blank=True)
    abc_fee_code = models.TextField( null=True, blank=True)
    abc_transfers = models.TextField( null=True, blank=True)
    abc_conditions = models.TextField(null=True, blank=True, default="N/A")
    abc_operating_restrictions = models.TextField(null=True, blank=True)
    abc_disciplinary_action = models.TextField(null=True, blank=True, default="No Active Disciplinary Action found")
    abc_disciplinary_history = models.TextField(null=True, blank=True, default="No Disciplinary History found.")
    abc_holds = models.TextField(null=True, blank=True, default="No Active Holds found")
    abc_escrows = models.TextField(null=True, blank=True, default="No Escrow found")
    abc_from_license_number = models.TextField( null=True, blank=True)
    abc_transferred_on = models.DateField(null=True, blank=True)
    abc_to_license_number = models.TextField( null=True, blank=True)
    abc_transferred_on2 = models.DateField(null=True, blank=True)
    # Data Scrapp From Google API
    google_business_name = models.TextField( null=True, blank=True)
    google_business_address = models.TextField( null=True, blank=True)
    google_place_name = models.TextField( null=True, blank=True)
    google_rating = models.TextField( null=True, blank=True)
    google_phone_number = models.TextField( null=True, blank=True)
    google_website = models.TextField(null=True, blank=True)
    google_types = models.TextField( null=True, blank=True)
    google_business_status = models.TextField( null=True, blank=True)
    data_Found_inContact_Info = models.BooleanField(default=False)
    data_Found_in_yelp = models.BooleanField(default=False)
    def __str__(self):
        return f"License Output Records : {self.abc_license_number} - {self.google_business_name}"
    
    
class YelpRestaurantRecord(models.Model):
    yelp_license_type = models.TextField(blank=True, null=True)
    yelp_file_number = models.TextField(blank=True, null=True) 
    yelp_primary_name = models.TextField(blank=True, null=True)
    yelp_dba_name = models.TextField(blank=True, null=True)
    yelp_prem_addr_1 = models.TextField( blank=True, null=True)
    yelp_prem_addr_2 = models.TextField( blank=True, null=True)
    yelp_prem_city = models.TextField(blank=True, null=True)
    yelp_prem_state = models.TextField(blank=True, null=True)
    yelp_prem_zip = models.TextField(blank=True, null=True)  
    yelp_link = models.TextField(blank=True, null=True)
    yelp_name = models.TextField( blank=True, null=True)
    yelp_phone = models.TextField( blank=True, null=True)
    yelp_web_site = models.TextField(blank=True, null=True) 
    yelp_rating = models.TextField( blank=True, null=True)
    def __str__(self):
        return f"Yelp Restaurant Records : {self.yelp_file_number} - {self.yelp_dba_name}"
# Model for Generating Data Set 1 End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

class CompanyInformationRecord(models.Model):
    Company_Info_License_Number = models.TextField(blank=True, null=True)
    Company_Info_Type = models.TextField(blank=True, null=True) 
    Company_Info_Name = models.TextField(blank=True, null=True)
    Company_Info_Role = models.TextField(blank=True, null=True)
    def __str__(self):
        return f"Company Information Record Restaurant Records : {self.Company_Info_License_Number} - {self.Company_Info_Name}"

# Model for Generating Data Set 2 Start ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
class PrincipalsInformation(models.Model):
        principalsInformation_entity_name = models.TextField(blank=True, null=True)
        principalsInformation_entity_num = models.TextField(blank=True, null=True)
        principalsInformation_org_name = models.TextField(blank=True, null=True)
        principalsInformation_first_name = models.TextField(blank=True, null=True)
        principalsInformation_middle_name = models.TextField( blank=True, null=True)
        principalsInformation_last_name = models.TextField(blank=True, null=True)
        principalsInformation_position_type = models.TextField(blank=True, null=True)
        principalsInformation_address1 = models.TextField(blank=True, null=True)
        principalsInformation_address2 = models.TextField( blank=True, null=True)
        principalsInformation_address3 = models.TextField( blank=True, null=True)
        principalsInformation_city = models.TextField(blank=True, null=True)
        principalsInformation_state = models.TextField(blank=True, null=True)
        principalsInformation_country = models.TextField(blank=True, null=True)
        principalsInformation_postal_code = models.TextField(max_length=20,blank=True, null=True)
        def __str__(self):
            return f"Principals Information {self.principalsInformation_first_name} {self.principalsInformation_last_name}"
        
class AgentsInformation(models.Model):
    agentsInformation_entity_name =  models.TextField(blank=True, null=True)
    agentsInformation_entity_num = models.TextField(blank=True, null=True)
    agentsInformation_org_name = models.TextField(blank=True, null=True)
    agentsInformation_first_name = models.TextField(blank=True, null=True)
    agentsInformation_middle_name = models.TextField( blank=True, null=True)
    agentsInformation_last_name = models.TextField(blank=True, null=True)
    agentsInformation_physical_address1 = models.TextField(blank=True, null=True)
    agentsInformation_physical_address2 = models.TextField( blank=True, null=True)
    agentsInformation_physical_address3 = models.TextField( blank=True, null=True)
    agentsInformation_physical_city = models.TextField(blank=True, null=True)
    agentsInformation_physical_state = models.TextField(blank=True, null=True)
    agentsInformation_physical_country = models.TextField(blank=True, null=True)
    agentsInformation_physical_postal_code = models.TextField(max_length=20,blank=True, null=True)
    agentsInformation_agent_type = models.TextField(blank=True, null=True)
    def __str__(self):
        return f"Agents Information {self.agentsInformation_entity_name}"
    

class FilingsInformation(models.Model):
    filingsInformation_entity_name = models.TextField(blank=True, null=True)
    filingsInformation_entity_num = models.TextField( blank=True, null=True)
    filingsInformation_initial_filing_date = models.TextField(blank=True, null=True)
    filingsInformation_jurisdiction = models.TextField(blank=True, null=True)
    filingsInformation_entity_status = models.TextField(blank=True, null=True)
    filingsInformation_standing_sos = models.TextField(blank=True, null=True)
    filingsInformation_entity_type = models.TextField(blank=True, null=True)
    filingsInformation_filing_type = models.TextField(max_length=60,blank=True, null=True)
    filingsInformation_foreign_name = models.TextField( blank=True, null=True)
    filingsInformation_standing_ftb = models.TextField( blank=True, null=True)
    filingsInformation_standing_vcfcf = models.TextField(blank=True, null=True)
    filingsInformation_standing_agent = models.TextField(blank=True, null=True)
    filingsInformation_suspension_date = models.TextField(blank=True, null=True)
    filingsInformation_last_si_file_number = models.TextField(max_length=70, blank=True, null=True)
    filingsInformation_last_si_file_date = models.TextField(blank=True, null=True)
    filingsInformation_principal_address = models.TextField( blank=True, null=True)
    filingsInformation_principal_address2 = models.TextField( blank=True, null=True)
    filingsInformation_principal_city = models.TextField( blank=True, null=True)
    filingsInformation_principal_state = models.TextField( blank=True, null=True)
    filingsInformation_principal_country = models.TextField( blank=True, null=True)
    filingsInformation_principal_postal_code= models.TextField( blank=True, null=True)
    filingsInformation_mailing_address= models.TextField( blank=True, null=True)
    filingsInformation_mailing_address2= models.TextField( blank=True, null=True)
    filingsInformation_mailing_address3= models.TextField( blank=True, null=True)
    filingsInformation_mailing_city= models.TextField( blank=True, null=True)
    filingsInformation_mailing_state= models.TextField( blank=True, null=True)
    filingsInformation_mailing_country= models.TextField( blank=True, null=True)
    filingsInformation_mailing_postal_code= models.TextField( blank=True, null=True)
    filingsInformation_principal_address_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_principal_address2_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_principal_city_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_principal_state_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_principal_country_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_principal_postal_code_in_ca= models.TextField( blank=True, null=True)
    filingsInformation_llc_management_structure = models.TextField( blank=True, null=True)
    filingsInformation_type_of_business = models.TextField( blank=True, null=True)
    def __str__(self):
        return f"Filings Information Records : {self.filingsInformation_entity_num} - {self.filingsInformation_entity_num}"
        
# Model for Generating Data Set 2 End ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
