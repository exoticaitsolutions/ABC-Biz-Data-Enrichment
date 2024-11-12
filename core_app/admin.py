
import csv
from django.shortcuts import render
from django import forms
from django.contrib import admin
from .models import *
from django.contrib import messages
from .forms import CSVImportForm
from io import TextIOWrapper
from django.urls import path
from django.template.response import TemplateResponse

class LicenseNumberAdmin(admin.ModelAdmin):
    # Specify the fields to display in the admin panel table
    list_display = ('license_number',)  # Replace with your model's fields
    list_filter = ('license_number',)  # Add filters to filter the data by these fields
    search_fields = ('license_number',)  # Enable search for these fields
    ordering = ('license_number',)
admin.site.register(LicenseNumber, LicenseNumberAdmin)
# Define the CSV upload form

@admin.register(CompanyInformation)
class CompanyInformationAdmin(admin.ModelAdmin):
    list_display = ("license_number", "type", "name", "role")
    
    # Adding custom actions to the actions dropdown

    def import_csv(self, request):
        """
        This action will allow the admin to upload a CSV to import data.
        """
        if request.method == "POST":
            form = CSVImportForm(request.POST, request.FILES)
            if form.is_valid():
                csv_file = TextIOWrapper(request.FILES["csv_file"].file, encoding="utf-8")
                reader = csv.DictReader(csv_file)
                try:
                    for row in reader:
                        # Update or create rows from the CSV data
                        CompanyInformation.objects.create(
                            license_number=row["license_number"],
                            defaults={
                                "type": row["type"],
                                "name": row["name"],
                                "role": row["role"],
                            },
                        )
                    self.message_user(request, "CSV imported successfully!", messages.SUCCESS)
                except Exception as e:
                    self.message_user(request, f"Error importing CSV: {str(e)}", messages.ERROR)
            else:
                self.message_user(request, "Invalid CSV file!", messages.ERROR)
        
        # Render the form for uploading CSV
        form = CSVImportForm()
        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "form": form,
        }
        return TemplateResponse(request, "admin/csv_form.html", context)

    import_csv.short_description = "Import CSV"

    def get_urls(self):
        """
        Overriding the default get_urls to add a URL for the CSV import action.
        """
        urls = super().get_urls()
        custom_urls = [
            path("import-csv/", self.import_csv, name="import_csv"),
        ]
        return custom_urls + urls
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        # Use the correct URL name 'admin:import_csv'
        extra_context['import_csv_url'] = 'admin:import_csv'
        return super().changelist_view(request, extra_context=extra_context)
 


# class CompanyInformationAdmin(admin.ModelAdmin):
#     list_display = ('license_number', 'type', 'name', 'role')  
#     list_filter = ('license_number', 'type', 'role')  
#     search_fields = ('license_number__name', 'type', 'name', 'role') 
#     ordering = ('license_number', 'type', 'name', 'role') 

# admin.site.register(CompanyInformation, CompanyInformationAdmin)


class LicenseOutputAdmin(admin.ModelAdmin):
    list_display = (
        'license_number', 'primary_owner', 'office_of_application', 'business_name',
        'county', 'license_type', 'license_type_status', 'status_date', 'term', 'expiration_date'
    )
    
    list_filter = ('license_type', 'status_date', 'county', 'license_type_status')

    search_fields = (
        'primary_owner', 'business_name', 'license_number__license_number', 'license_type',
        'license_type_status', 'county'
    )

    ordering = ('license_number', 'status_date')

    fieldsets = (
        (None, {
            'fields': ('license_number', 'primary_owner', 'office_of_application', 'business_name')
        }),
        ('License Details', {
            'fields': ('license_type', 'license_type_status', 'status_date', 'term', 'expiration_date')
        }),
        ('Business Information', {
            'fields': ('business_name_alt', 'business_address', 'place_name', 'phone_number', 'website')
        }),
        ('Transfer Details', {
            'fields': ('from_license_number', 'transferred_on', 'to_license_number', 'transferred_on2')
        }),
        ('Additional Information', {
            'fields': ('conditions', 'operating_restrictions', 'disciplinary_action', 'holds', 'escrows')
        }),
    )

    # You can also include readonly fields if needed
    readonly_fields = ('license_number', 'status_date', 'term')

admin.site.register(LicenseOutput, LicenseOutputAdmin)

    
from django.contrib import admin
from .models import AbcBizYelpRestaurantData

class AbcBizYelpRestaurantDataAdmin(admin.ModelAdmin):
    # Fields to display in the list view
    list_display = (
        'file_number', 'primary_name', 'dba_name', 'prem_addr_1', 'prem_city', 'prem_state', 
        'prem_zip', 'yelp_rating', 'yelp_name', 'yelp_phone', 'yelp_link', 'yelp_web_site'
    )

    # Fields to filter by in the sidebar
    list_filter = ('prem_state', 'license_type', 'file_number')

    # Fields to search by in the search bar
    search_fields = ('primary_name', 'dba_name', 'prem_addr_1', 'prem_addr_2', 'prem_city', 'prem_state', 'yelp_name', 'yelp_phone')

    # Default ordering for the list
    ordering = ('prem_state', 'prem_city', 'primary_name')

    # Optional: Customize how the data is displayed in the admin interface
    def get_readonly_fields(self, request, obj=None):
        return ['file_number']

# Register the model and admin class
admin.site.register(AbcBizYelpRestaurantData, AbcBizYelpRestaurantDataAdmin)


# admin.site.register(LicenseOutput)
# admin.site.register(AbcBizYelpRestaurantData)
admin.site.register(AgentsInformation)
admin.site.register(FilingsInformation)
admin.site.register(PrincipalsInformation)
admin.site.register(LicenseeNameDataEnrichment)


# admin.site.register(LicenseNumber)
# admin.site.register(CompanyInformation)

