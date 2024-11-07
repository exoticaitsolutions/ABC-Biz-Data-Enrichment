import datetime
from django.db import models
from datetime import datetime


class LicenseNumber(models.Model):
    license_number = models.IntegerField()

    def __str__(self):
        return str(self.license_number)


class CompanyInformation(models.Model):
    license_number = models.IntegerField()
    type = models.CharField(max_length=100)
    name = models.CharField(max_length=255)
    role = models.CharField(max_length=100)

    def __str__(self):
        return str(self.license_number)
    

class LicenseOutput(models.Model):
    license_number = models.IntegerField()
    primary_owner = models.CharField(max_length=255)
    office_of_application = models.CharField(max_length=255)
    business_name = models.CharField(max_length=255)
    business_address = models.CharField(max_length=255)
    county = models.CharField(max_length=100)
    census_tract = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    licensee = models.CharField(max_length=255)
    license_type = models.CharField(max_length=100)
    license_type_status = models.CharField(max_length=100)
    status_date = models.DateField()
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
    website = models.URLField(null=True, blank=True)
    types = models.CharField(max_length=255)
    business_status = models.CharField(max_length=100)

    def __str__(self):
        return f"License Number: {self.license_number} - {self.business_name}"
