
import time
from django.contrib import admin,messages
from django.db.models import CharField
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from ABC_BizEnrichment.common.logconfig import logger
from django.db.models import F, Value
from django.db.models.functions import Replace, Lower
from core_app.models import AgentsInformation, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import  DataErichmentWithoutConpanyInfo, DataSet1Record, DataSet2Record

@admin.register(DataSet1Record)
class DataSet1RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):  # ExportMixin
    merge_url_name = "export_data_set1"
    actions = ["merge_and_import_data_action"]
    search_fields = ['abc_license_number', 'yelp_file_number', "abc_licensee", "yelp_primary_name"]
    list_display = ("id", "abc_license_number", "yelp_file_number", "abc_licensee", "yelp_primary_name", 
                    "output_license_file_status", "yelp_file_status")
    license_output_all_columns = get_column_names(LicenseOutput, ['id'], include_relations=True)
    yelp_output_all_columns = get_column_names(YelpRestaurantRecord, ['id'], include_relations=True)
    def get_merge_view(self):
        def MergeDataSet1RecordsAdmin(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for LicenseOutput & AbcBizYelpRestaurantData and saved in the Dataset 1 (Business License)'
            
            license_output_qs = LicenseOutput.objects.all()
            total_license_count = license_output_qs.count()
            dataset1_records_to_create = []
            logger.debug(f"{full_function_name}: total number of Yelp Restaurant Records data: {total_license_count}")

            # # Fetch all LicenseOutput records at once and create a dictionary for fast lookup
            yelp_dict = {lo.yelp_file_number: lo for lo in YelpRestaurantRecord.objects.all()}

            # Use tqdm for a progress bar while iterating through all the records
            for batch_license_output_data in tqdm(license_output_qs, desc="Merging Data", total=total_license_count, unit="record"):
                dataset1records = DataSet1Record()
                license_number = batch_license_output_data.abc_license_number                    
                yelp_output = yelp_dict.get(license_number)
                if yelp_output:
                    logger.debug(f'Found in the yelp Records == {license_number}')
                    for column_nameq in self.yelp_output_all_columns:
                        if hasattr(yelp_output, column_nameq):
                            setattr(dataset1records, column_nameq, getattr(yelp_output, column_nameq))
                    dataset1records.yelp_file_status = True
                    for column_name in self.license_output_all_columns:
                        if hasattr(batch_license_output_data, column_name):
                            setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                    dataset1records.output_license_file_status = True
                else:
                    logger.debug(f'Not Found in the yelp == {license_number}')
                    for column_name in self.license_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                    dataset1records.output_license_file_status = False
                    dataset1records.output_license_file_status = True
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
    # search_fields = ("agentsInformation_entity_num","principalsInformation_entity_num")
    agents_information_all_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    principalsinformation_all_columns = get_column_names(PrincipalsInformation, ['id'], include_relations=True)
    # fieldsets = (
    #     ('Principals Information Record', {
    #         'classes': ('collapse',),
    #         'fields': tuple(principalsinformation_all_columns),
    #     }),
    #     ('Agents Information Record', {
    #         'classes': ('collapse',),
    #         'fields': tuple(agents_information_all_columns),
    #     }),
    #     ('Filings Information Record', {
    #         'classes': ('collapse',),   
    #         'fields': tuple(filings_Information_all_columns),
    #     }),
    #     ('Additional Fields Record', {
    #         'fields': ('filling_information_file_status', 'principal_information_file_status','agentsInformation_file_status'),
    #     }),
    # )

    # list_display = ("id", "principalsInformation_entity_num", "principalsInformation_entity_name", 
    #                 "principalsInformation_first_name", "principalsInformation_last_name", 
    #                 "filling_information_file_status", "principal_information_file_status", 
    #                 "agentsInformation_file_status")
    # search_fields = ['principalsInformation_entity_name']  # You can add other fields here as needed
    
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            fetch_filingsinformation = FilingsInformation.objects.all()
            total_filingsinformation_count = fetch_filingsinformation.count()
            logger.debug(f"{full_function_name}: total number of Filings Information data: {total_filingsinformation_count}")
            # Fetch all related records in bulk (for faster lookup)
            agents_info_dict = {ai.agentsInformation_entity_name: ai for ai in AgentsInformation.objects.all()}
            principal_info_dict = {fi.principalsInformation_entity_name: fi for fi in PrincipalsInformation.objects.all()}
            records_to_create = []
            # Use tqdm for a progress bar while iterating through all the records
            for filingsinformationdata in tqdm(fetch_filingsinformation, desc="Merging Data", total=total_filingsinformation_count, unit="record"):
                entity_name = str(filingsinformationdata.filingsInformation_entity_name)                
                dataset2records = DataSet2Record()
                # Lookup related records in the dictionaries
                agent_informations = agents_info_dict.get(entity_name)
                principalsinformation = principal_info_dict.get(entity_name)

                # If found in the Agents Information, copy the data to the dataset2records instance
                if agent_informations:
                    for column_name in self.agents_information_all_columns:
                        if hasattr(agent_informations, column_name):
                            setattr(dataset2records, column_name, getattr(agent_informations, column_name))
                    # Copy filings information data to the dataset2records instance
                    for column_name in self.filings_Information_all_columns:
                        if hasattr(filingsinformationdata, column_name):
                            setattr(dataset2records, column_name, getattr(filingsinformationdata, column_name))
                    dataset2records.filling_information_file_status = True
                    dataset2records.agentsInformation_file_status = True

                # If found in the Principals Information, copy the data to the dataset2records instance
                if principalsinformation:
                    for column_name in self.principalsinformation_all_columns:
                        if hasattr(principalsinformation, column_name):
                            setattr(dataset2records, column_name, getattr(principalsinformation, column_name))
                    dataset2records.principal_information_file_status = True
                    # Copy filings information data to the dataset2records instance
                    for column_name in self.filings_Information_all_columns:
                        if hasattr(filingsinformationdata, column_name):
                            setattr(dataset2records, column_name, getattr(filingsinformationdata, column_name))
                    dataset2records.filling_information_file_status = True

                records_to_create.append(dataset2records)
                # Bulk insert every 500 records (or another suitable number)
                if len(records_to_create) >= 500:
                    DataSet2Record.objects.bulk_create(records_to_create)
                    records_to_create.clear()
                    time.sleep(2)
            if records_to_create:
                DataSet2Record.objects.bulk_create(records_to_create)
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # Redirect after processing
        return DataSet2Recordmerge_view
@admin.register(DataErichmentWithoutConpanyInfo)
class DataErichmentWithoutConpanyInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "dataerichmentrecordswithout_conpany_info"
    list_display = (
        "id", "abc_license_number", "abc_licensee", "agentsInformation_entity_name",
        "principalsInformation_first_name", "principalsInformation_last_name",
        "data_set_1_file_status", "data_set_2_file_status"
    )
    search_fields = ("agentsInformation_entity_num", "principalsInformation_entity_num")

    def get_merge_view(self):
        def DataSet3Recordmerge_view(request):
            full_function_name = get_full_function_name()

            data_set_1_columns = get_column_names(DataSet1Record, ['id', 'license_type', 'file_number'], include_relations=True)
            data_set_2_columns = get_column_names(DataSet2Record, ['id', 'license_type', 'file_number'], include_relations=True)

            batch_size = 1000
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in Dataset 2 (Combined Information)'

            norm_first = DataSet1Record.objects.annotate(
                normalized_name=Lower(
                    Replace(
                        Replace(
                            Replace(F('abc_licensee'), Value('|'), Value('')),
                            Value(','), Value('')
                        ),
                        Value('.'), Value(''),
                        output_field=CharField()
                    )
                )
            )

            norm_second = DataSet2Record.objects.annotate(
                normalized_name=Lower(
                    Replace(
                        Replace(
                            Replace(F('agentsInformation_entity_name'), Value('|'), Value('')),
                            Value(','), Value('')
                        ),
                        Value('.'), Value(''),
                        output_field=CharField()
                    )
                )
            )

            total = norm_first.count()
            total_batches = (total + batch_size - 1) // batch_size

            # Cache second table in-memory dict for ultra-fast lookup
            second_lookup = {
                s.normalized_name: [] for s in norm_second
            }
            for s in norm_second:
                second_lookup[s.normalized_name].append(s)  
            with tqdm(total=total_batches, desc="Merging", unit="batch") as pbar:
                for i in range(0, total, batch_size):
                    batch = norm_first[i:i + batch_size]
                    to_create = []
                    for f in batch:
                        norm_name = str(f.normalized_name)
                        if norm_name:
                            matches = second_lookup.get(norm_name, [])
                            if matches:
                                for s in matches:
                                    merged = DataErichmentWithoutConpanyInfo()
                                    for c in data_set_2_columns:
                                        if hasattr(s, c):
                                            setattr(merged, c, getattr(s, c))
                                    merged.data_set_2_file_status = True
                                    for c in data_set_1_columns:
                                        if hasattr(f, c):
                                            setattr(merged, c, getattr(f, c))
                                    merged.data_set_1_file_status = True
                                    to_create.append(merged)
                            else:
                                merged = DataErichmentWithoutConpanyInfo()
                                for c in data_set_1_columns:
                                    if hasattr(f, c):
                                        setattr(merged, c, getattr(f, c))
                                merged.data_set_1_file_status = True
                                to_create.append(merged)
                    if to_create:
                        DataErichmentWithoutConpanyInfo.objects.bulk_create(to_create, batch_size=batch_size)
                        time.sleep(2)
                    pbar.update(1)
            logger.info(f"{full_function_name}: Done merging {total} records.")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataerichmentwithoutconpanyinfo/")

        return DataSet3Recordmerge_view