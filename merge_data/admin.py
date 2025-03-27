
import csv
import time
from django.contrib import admin,messages
from django.db.models import CharField
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import generate_model_fields, get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from ABC_BizEnrichment.common.logconfig import logger
from django.db.models import F, Value
from django.db.models.functions import Replace, Lower
from core_app.models import AgentsInformation, CompanyInformationRecord, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import  DataErichmentWithConpanyInfo, DataErichmentWithoutConpanyInfo, DataSet1Record, DataSet2Record
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
            
            license_output_qs = YelpRestaurantRecord.objects.all()
            total_license_count = license_output_qs.count()
            dataset1_records_to_create = []
            logger.debug(f"{full_function_name}: total number of Yelp Restaurant Records data: {total_license_count}")

            # Fetch all LicenseOutput records at once and create a dictionary for fast lookup
            license_outputs_dict = {lo.abc_license_number: lo for lo in LicenseOutput.objects.all()}

            # Use tqdm for a progress bar while iterating through all the records
            for batch_license_output_data in tqdm(license_output_qs, desc="Merging Data", total=total_license_count, unit="record"):
                dataset1records = DataSet1Record()
                license_number = batch_license_output_data.yelp_file_number                    
                yelp_output = license_outputs_dict.get(license_number)

                if yelp_output:
                    for column_nameq in self.license_output_all_columns:
                        if hasattr(yelp_output, column_nameq):
                            setattr(dataset1records, column_nameq, getattr(yelp_output, column_nameq))
                    dataset1records.output_license_file_status = True
                    
                for column_name in self.yelp_output_all_columns:
                    if hasattr(batch_license_output_data, column_name):
                        setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                dataset1records.yelp_file_status = True

                # Collect the records for bulk creation
                dataset1_records_to_create.append(dataset1records)

                # Save in bulk after processing every 500 records (or another suitable batch size)
                if len(dataset1_records_to_create) >= 500:
                    DataSet1Record.objects.bulk_create(dataset1_records_to_create)
                    dataset1_records_to_create = []  # Clear the list after bulk create

            # Bulk save any remaining records
            if dataset1_records_to_create:
                DataSet1Record.objects.bulk_create(dataset1_records_to_create)

            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset1record/")

        return MergeDataSet1RecordsAdmin

    

@admin.register(DataSet2Record)
class DataSet2RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "data_set2_records"
    
    agents_information_all_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    principalsinformation_all_columns = get_column_names(PrincipalsInformation, ['id'], include_relations=True)
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

    list_display = ("id", "principalsInformation_entity_num", "principalsInformation_entity_name", 
                    "principalsInformation_first_name", "principalsInformation_last_name", 
                    "filling_information_file_status", "principal_information_file_status", 
                    "agentsInformation_file_status")
    search_fields = ['principalsInformation_entity_name']  # You can add other fields here as needed
    
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            fetch_principalsinformation = PrincipalsInformation.objects.all()
            total_principalsinformation_count = fetch_principalsinformation.count()        
            # Fetch all related records in bulk (for faster lookup)
            agents_info_dict = {ai.agentsInformation_entity_name: ai for ai in AgentsInformation.objects.all()}
            filings_info_dict = {fi.filingsInformation_entity_name: fi for fi in FilingsInformation.objects.all()}
            records_to_create = []
            # Use tqdm for a progress bar while iterating through all the records
            for principalsinformationdata in tqdm(fetch_principalsinformation, desc="Merging Data", total=total_principalsinformation_count, unit="record"):
                entity_name = str(principalsinformationdata.principalsInformation_entity_name)                
                dataset2records = DataSet2Record()
                
                # Lookup related records in the dictionaries
                agent_informations = agents_info_dict.get(entity_name)
                filingsinformation = filings_info_dict.get(entity_name)
                # If found in the Agents Information, copy the data to the dataset2records instance
                if agent_informations:
                    for column_name in self.agents_information_all_columns:
                        if hasattr(agent_informations, column_name):
                            setattr(dataset2records, column_name, getattr(agent_informations, column_name))
                    dataset2records.agentsInformation_file_status = True

                # If found in the Filings Information, copy the data to the dataset2records instance
                if filingsinformation:
                    for column_name in self.filings_Information_all_columns:
                        if hasattr(filingsinformation, column_name):
                            setattr(dataset2records, column_name, getattr(filingsinformation, column_name))
                    dataset2records.filling_information_file_status = True

                # Copy principals information data to the dataset2records instance
                for column_name in self.principalsinformation_all_columns:
                    if hasattr(principalsinformationdata, column_name):
                        setattr(dataset2records, column_name, getattr(principalsinformationdata, column_name))
                dataset2records.principal_information_file_status = True

                records_to_create.append(dataset2records)
                # Bulk insert every 500 records (or another suitable number)
                if len(records_to_create) >= 500:
                    DataSet2Record.objects.bulk_create(records_to_create)
                    records_to_create.clear()
                    time.sleep(2)  # Optional: delay to avoid overwhelming the database
            # Bulk save any remaining records after loop finishes
            if records_to_create:
                DataSet2Record.objects.bulk_create(records_to_create)
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # Redirect after processing
        return DataSet2Recordmerge_view
@admin.register(DataErichmentWithoutConpanyInfo)
class DataErichmentWithoutConpanyInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "dataerichmentrecordswithout_conpany_info"
    list_display = ("id", "abc_license_number", "abc_licensee", "agentsInformation_entity_name", "data_set_1_file_status", "data_set_2_file_status")

    def get_merge_view(self):
        def DataSet3Recordmerge_view(request):
            full_function_name = get_full_function_name()
            data_set_1_all_columns = get_column_names(DataSet1Record, ['id', 'license_type', 'file_number'], include_relations=True)
            data_set_2_all_columns = get_column_names(DataSet2Record, ['id', 'license_type', 'file_number'], include_relations=True)
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in Dataset 2 (Combined Information)'

            # Fetch all records from both datasets with pre-normalized names
            normalized_first_table = DataSet1Record.objects.annotate(
                normalized_name=Lower(
                    Replace(
                        Replace(
                            Replace(
                                F('abc_licensee'),
                                Value('|'), Value('')
                            ),
                            Value(','), Value('')
                        ),
                        Value('.'), Value(''),
                        output_field=CharField()
                    )
                )
            )

            normalized_second_table = DataSet2Record.objects.annotate(
                normalized_name=Lower(
                    Replace(
                        Replace(
                            Replace(
                                F('agentsInformation_entity_name'),
                                Value('|'), Value('')
                            ),
                            Value(','), Value('')
                        ),
                        Value('.'), Value(''),
                        output_field=CharField()
                    )
                )
            )

            total_data_Set_1_count = normalized_first_table.count()
            logger.info(f"{full_function_name}: total number of data set 1 data: {total_data_Set_1_count}")
            
            records_to_create = []  # List to accumulate records for bulk insertion

            # Create a dictionary of normalized second dataset records to avoid querying inside the loop
            second_data_dict = {record.normalized_name: record for record in normalized_second_table}

            # Use tqdm for a progress bar while iterating through the records
            for first_record in tqdm(normalized_first_table, desc="Merging Data", total=total_data_Set_1_count, unit="record"):
                licensee = str(first_record.normalized_name)
                
                # Retrieve the matching second record if it exists in the dict (no need for multiple database queries)
                matching_second_record = second_data_dict.get(licensee)
                
                if matching_second_record:
                    dataset3records = DataErichmentWithoutConpanyInfo()
                    
                    # Transfer columns from the second dataset
                    for column_name in data_set_2_all_columns:
                        if hasattr(matching_second_record, column_name):
                            setattr(dataset3records, column_name, getattr(matching_second_record, column_name))
                    
                    dataset3records.data_set_2_file_status = True
                    
                    # Transfer columns from the first dataset
                    for column_name in data_set_1_all_columns:
                        if hasattr(first_record, column_name):
                            setattr(dataset3records, column_name, getattr(first_record, column_name))
                    
                    dataset3records.data_set_1_file_status = True
                    records_to_create.append(dataset3records)
                else:
                    dataset3records = DataErichmentWithoutConpanyInfo()
                    # Transfer columns from the first dataset
                    for column_name in data_set_1_all_columns:
                        if hasattr(first_record, column_name):
                            setattr(dataset3records, column_name, getattr(first_record, column_name))
                    dataset3records.data_set_1_file_status = True
                    records_to_create.append(dataset3records)

                if len(records_to_create) >= 500:
                    DataErichmentWithoutConpanyInfo.objects.bulk_create(records_to_create)
                    records_to_create.clear()  # Clear the list after bulk create
            # Final bulk insert after loop
            if records_to_create:
                DataErichmentWithoutConpanyInfo.objects.bulk_create(records_to_create)
            # Show success message after processing
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataerichmentwithoutconpanyinfo/")  # Redirect after processing

        return DataSet3Recordmerge_view
    

@admin.register(DataErichmentWithConpanyInfo)
class DataErichmentWithConpanyInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "dataerichmentrecordswith_conpany_info"
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            fetch_dataerichmentwithoutconpanyinfo = DataErichmentWithoutConpanyInfo.objects.all()
            total_principalsinformation_count = fetch_dataerichmentwithoutconpanyinfo.count()   
            data_set_2_all_columns = get_column_names(DataErichmentWithConpanyInfo, ['id'], include_relations=True)
            records_to_create = []
            for principalsinformationdata in tqdm(fetch_dataerichmentwithoutconpanyinfo, desc="Merging Data", total=total_principalsinformation_count, unit="record"):
                abc_license_number = str(principalsinformationdata.abc_license_number)  
                matching_second_records = CompanyInformationRecord.objects.filter(Company_Info_License_Number=abc_license_number)
                if matching_second_records:
                    for column_name1 in matching_second_records:
                        dataset3records = DataErichmentWithConpanyInfo()
                        for column_name in data_set_2_all_columns:
                            if hasattr(column_name1, column_name):
                                setattr(dataset3records, column_name, getattr(column_name1, column_name))
                        for column_name in data_set_2_all_columns:
                            if hasattr(principalsinformationdata, column_name):
                                setattr(dataset3records, column_name, getattr(principalsinformationdata, column_name))
                        records_to_create.append(dataset3records)
                        # dataset3records.save()
                else:
                    dataset3records = DataErichmentWithConpanyInfo()
                    for column_name in data_set_2_all_columns:
                            if hasattr(principalsinformationdata, column_name):
                                setattr(dataset3records, column_name, getattr(principalsinformationdata, column_name))
                    # dataset3records.save()
                    records_to_create.append(dataset3records)
                if len(records_to_create) >= 500:
                    DataErichmentWithConpanyInfo.objects.bulk_create(records_to_create)
                    records_to_create.clear()  # Clear the list after bulk create
                # break
            if records_to_create:
                DataErichmentWithConpanyInfo.objects.bulk_create(records_to_create)
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataerichmentwithconpanyinfo/")  # Redirect after processing
        return DataSet2Recordmerge_view