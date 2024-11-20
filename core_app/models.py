import datetime
from django.db import models
from datetime import datetime


class LicenseNumber(models.Model):
    license_number = models.IntegerField(unique=True)

    def __str__(self):
        return str(self.license_number)


class CompanyInformation(models.Model):
    license_number=models.ForeignKey(LicenseNumber, on_delete=models.CASCADE, null=True, 
    blank=True)
    type = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=100)

    def __str__(self):
        return str(self.license_number)
    

class LicenseOutput(models.Model):
    license_number=models.ForeignKey(LicenseNumber, on_delete=models.CASCADE, null=True, 
    blank=True)
    primary_owner = models.CharField(max_length=255)
    office_of_application = models.CharField(max_length=255)
    business_name = models.CharField(max_length=255)
    business_address = models.CharField(max_length=255)
    county = models.CharField(max_length=100)
    census_tract = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    licensee = models.CharField(max_length=255)
    license_type = models.CharField(max_length=100)
    license_type_status = models.CharField(max_length=100)
    status_date = models.DateField(null=True, blank=True)
    term = models.CharField(max_length=100)
    original_issue_date = models.DateField(null=True, blank=True)
    expiration_date = models.DateField(null=True, blank=True)
    master = models.CharField(max_length=100)
    duplicate = models.IntegerField(null=True, blank=True)
    fee_code = models.CharField(max_length=100)
    transfers = models.CharField(max_length=255)
    conditions = models.TextField(null=True, blank=True)
    operating_restrictions = models.TextField(null=True, blank=True)
    disciplinary_action = models.TextField(null=True, blank=True)
    disciplinary_history = models.TextField(null=True, blank=True)
    holds = models.TextField(null=True, blank=True)
    escrows = models.TextField(null=True, blank=True)
    from_license_number = models.CharField(max_length=20, null=True, blank=True)
    transferred_on = models.DateField(null=True, blank=True)
    to_license_number = models.CharField(max_length=20, null=True, blank=True)
    transferred_on2 = models.DateField(null=True, blank=True)
    business_name_alt = models.CharField(max_length=255)
    business_address_alt = models.CharField(max_length=255)
    place_name = models.CharField(max_length=255)
    rating = models.CharField(max_length=50)
    phone_number = models.CharField(max_length=50)
    website = models.URLField(null=True,blank=True)
    types = models.CharField(max_length=255)
    business_status = models.CharField(max_length=100)

    def __str__(self):
        return f"License Number: {self.license_number} - {self.business_name}"

class AbcBizYelpRestaurantData(models.Model):
    license_type = models.CharField(max_length=100)
    file_number = models.ForeignKey(LicenseNumber, on_delete=models.CASCADE, null=True, 
    blank=True)
    primary_name = models.CharField(max_length=200,blank=True, null=True)
    dba_name = models.CharField(max_length=200)
    prem_addr_1 = models.CharField(max_length=200)
    prem_addr_2 = models.CharField(max_length=200, blank=True, null=True)
    prem_city = models.CharField(max_length=100)
    prem_state = models.CharField(max_length=50)
    prem_zip = models.CharField(max_length=20)  
    yelp_link = models.URLField(blank=True, null=True)
    yelp_name = models.CharField(max_length=200, blank=True, null=True)
    yelp_phone = models.CharField(max_length=20, blank=True, null=True)
    yelp_web_site = models.URLField(blank=True, null=True)
    yelp_rating = models.CharField(max_length=200, blank=True, null=True)

    def __str__(self):
        return self.primary_name

class LicenseeNameDataEnrichment(models.Model):
    license_number = models.ForeignKey(LicenseNumber, on_delete=models.CASCADE, null=True, 
    blank=True)   
    master = models.CharField(max_length=100,blank=True, null=True)
    name = models.CharField(max_length=200,blank=True, null=True)
    entity_num = models.CharField(max_length=200,blank=True, null=True)
    is_entity_num = models.BooleanField(default= False)
    role = models.CharField(max_length=100,blank=True, null=True)
    licensee = models.CharField(max_length=200,blank=True, null=True)
    business_name = models.CharField(max_length=200,blank=True, null=True)
    website = models.URLField(blank=True, null=True)
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    last_name = models.CharField(max_length=100,blank=True, null=True)
    first_name = models.CharField(max_length=100,blank=True, null=True)
    middle_name = models.CharField(max_length=100, blank=True, null=True)
    second_middle = models.CharField(max_length=100, blank=True, null=True)
    suffix = models.CharField(max_length=50, blank=True, null=True)
    full_name = models.CharField(max_length=200,blank=True, null=True)
    validated_work_email = models.EmailField(blank=True, null=True)

    def __str__(self):
        return self.name
class AgentsInformation(models.Model):
    entity_name =  models.CharField(max_length=200,blank=True, null=True)
    # entity_num = models.ForeignKey(LicenseeNameDataEnrichment, on_delete=models.CASCADE, null=True, 
    # blank=True)
    entity_num = models.CharField(max_length=100, blank=True, null=True)
    org_name = models.CharField(max_length=200,blank=True, null=True)
    first_name = models.CharField(max_length=100,blank=True, null=True)
    middle_name = models.CharField(max_length=100, blank=True, null=True)
    last_name = models.CharField(max_length=100,blank=True, null=True)
    physical_address1 = models.CharField(max_length=200,blank=True, null=True)
    physical_address2 = models.CharField(max_length=200, blank=True, null=True)
    physical_address3 = models.CharField(max_length=200, blank=True, null=True)
    physical_city = models.CharField(max_length=100,blank=True, null=True)
    physical_state = models.CharField(max_length=50,blank=True, null=True)
    physical_country = models.CharField(max_length=50,blank=True, null=True)
    physical_postal_code = models.CharField(max_length=20,blank=True, null=True)
    agent_type = models.CharField(max_length=100,blank=True, null=True)

    def __str__(self):
        return self.entity_name
    
class FilingsInformation(models.Model):
    license_type = models.CharField(max_length=100)
    file_number = models.CharField(max_length=100)
    lic_or_app = models.CharField(max_length=100)
    type_status = models.CharField(max_length=100)
    type_orig_iss_date = models.DateField(blank=True, null=True)
    expir_date = models.DateField(blank=True, null=True)
    fee_codes = models.CharField(max_length=100, blank=True, null=True)
    dup_counts = models.IntegerField(blank=True, null=True)
    master_ind = models.CharField(max_length=50)
    term_in_number_of_months = models.IntegerField(blank=True, null=True)
    geo_code = models.CharField(max_length=50, blank=True, null=True)
    district = models.CharField(max_length=50, blank=True, null=True)
    primary_name = models.CharField(max_length=200,blank=True, null=True)
    prem_addr_1 = models.CharField(max_length=200,blank=True, null=True)
    prem_addr_2 = models.CharField(max_length=200, blank=True, null=True)
    prem_city = models.CharField(max_length=100,blank=True, null=True)
    prem_state = models.CharField(max_length=50,blank=True, null=True)
    prem_zip = models.CharField(max_length=20,blank=True, null=True)
    dba_name = models.CharField(max_length=200, blank=True, null=True)
    mail_addr_1 = models.CharField(max_length=200,blank=True, null=True)
    mail_addr_2 = models.CharField(max_length=200, blank=True, null=True)
    mail_city = models.CharField(max_length=100,blank=True, null=True)
    mail_state = models.CharField(max_length=50,blank=True, null=True)
    mail_zip = models.CharField(max_length=20,blank=True, null=True)
    prem_county = models.CharField(max_length=100, blank=True, null=True)
    prem_census_tract = models.CharField(max_length=50, blank=True, null=True)
    # entity_num = models.ForeignKey(LicenseeNameDataEnrichment, on_delete=models.CASCADE, null=True, 
    # blank=True)
    entity_num = models.CharField(max_length=100, blank=True, null=True)
    initial_filing_date = models.DateField(blank=True, null=True)
    jurisdiction = models.CharField(max_length=100, blank=True, null=True)
    entity_status = models.CharField(max_length=100)
    standing_sos = models.CharField(max_length=100, blank=True, null=True)
    entity_type = models.CharField(max_length=100,blank=True, null=True)
    filing_type = models.CharField(max_length=100,blank=True, null=True)
    foreign_name = models.CharField(max_length=200, blank=True, null=True)
    standing_ftb = models.CharField(max_length=100, blank=True, null=True)
    standing_vcfcf = models.CharField(max_length=100, blank=True, null=True)
    standing_agent = models.CharField(max_length=100, blank=True, null=True)
    suspension_date = models.DateField(blank=True, null=True)
    last_si_file_number = models.CharField(max_length=100, blank=True, null=True)
    last_si_file_date = models.DateField(blank=True, null=True)
    llc_management_structure = models.CharField(max_length=100, blank=True, null=True)
    type_of_business = models.CharField(max_length=200, blank=True, null=True)

    def __str__(self):
        return self.primary_name
    
class PrincipalsInformation(models.Model):
    entity_name = models.CharField(max_length=200,blank=True, null=True)
    # entity_num = models.ForeignKey(LicenseeNameDataEnrichment, on_delete=models.CASCADE, null=True, 
    # blank=True)
    entity_num = models.CharField(max_length=100, blank=True, null=True)
    org_name = models.CharField(max_length=200,blank=True, null=True)
    first_name = models.CharField(max_length=100,blank=True, null=True)
    middle_name = models.CharField(max_length=100, blank=True, null=True)
    last_name = models.CharField(max_length=100,blank=True, null=True)
    address1 = models.CharField(max_length=200,blank=True, null=True)
    address2 = models.CharField(max_length=200, blank=True, null=True)
    address3 = models.CharField(max_length=200, blank=True, null=True)
    city = models.CharField(max_length=100,blank=True, null=True)
    state = models.CharField(max_length=50,blank=True, null=True)
    country = models.CharField(max_length=50,blank=True, null=True)
    postal_code = models.CharField(max_length=20,blank=True, null=True)
    position_1 = models.CharField(max_length=100,blank=True, null=True)
    position_2 = models.CharField(max_length=100,blank=True, null=True)
    position_3 = models.CharField(max_length=100,blank=True, null=True)
    position_4 = models.CharField(max_length=100,blank=True, null=True)
    position_5 = models.CharField(max_length=100,blank=True, null=True)
    position_6 = models.CharField(max_length=100,blank=True, null=True)
    position_7 = models.CharField(max_length=100,blank=True, null=True)
    # From Old CSV
    # principal_address = models.CharField(max_length=200,blank=True, null=True)
    # principal_address2 = models.CharField(max_length=200, blank=True, null=True)
    # principal_city = models.CharField(max_length=100,blank=True, null=True)
    # principal_state = models.CharField(max_length=50,blank=True, null=True)
    # principal_country = models.CharField(max_length=50,blank=True, null=True)
    # principal_postal_code = models.CharField(max_length=20,blank=True, null=True)
    # principal_address_in_ca = models.CharField(max_length=200, blank=True, null=True)
    # principal_address2_in_ca = models.CharField(max_length=200, blank=True, null=True)
    # principal_city_in_ca = models.CharField(max_length=100, blank=True, null=True)
    # principal_state_in_ca = models.CharField(max_length=50, blank=True, null=True)
    # principal_country_in_ca = models.CharField(max_length=50, blank=True, null=True)
    # principal_postal_code_in_ca = models.CharField(max_length=20, blank=True, null=True)

    def __str__(self):
        return f"{self.first_name} {self.last_name}"
    



    