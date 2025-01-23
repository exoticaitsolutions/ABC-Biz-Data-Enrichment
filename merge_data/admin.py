
import time
from django.contrib import admin,messages
from django.db.models import CharField
from ABC_BizEnrichment.common.helper_function import generate_model_fields, get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from ABC_BizEnrichment.common.logconfig import logger
from django.db.models import F, Value
from django.db.models.functions import Replace, Lower
from core_app.models import AgentsInformation, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import DataErichment, DataSet1Record, DataSet2Record
from import_export.admin import ExportMixin

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
            batch_size = 300
            license_output_qs = YelpRestaurantRecord.objects.all()
            total_license_count = license_output_qs.count()
            dataset1_records_to_create = []
            logger.info(f"{full_function_name}: total number of Yelp Restaurant Records data: {total_license_count}")

            for i in range(0, total_license_count, batch_size):
                batch_license_output_ = license_output_qs[i:i + batch_size]
                for batch_license_output_data in batch_license_output_:
                    dataset1records = DataSet1Record()
                    license_number = batch_license_output_data.yelp_file_number
                    logger.info(f"{full_function_name}: Processing license_number: {license_number}")
                    
                    yelp_output = LicenseOutput.objects.filter(abc_license_number=license_number).first()
                    if yelp_output:
                        logger.info(f"{full_function_name}: Records Found in the yelp_output {license_number}")
                        for column_nameq in self.license_output_all_columns:
                            if hasattr(yelp_output, column_nameq):
                                setattr(dataset1records, column_nameq, getattr(yelp_output, column_nameq))
                        dataset1records.output_license_file_status = True
                        for column_name in self.yelp_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                        dataset1records.yelp_file_status = True
                    else:
                        logger.info(f"{full_function_name}: Records are not found in the yelp_output {license_number}")
                        for column_name in self.yelp_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                        dataset1records.yelp_file_status = True
                    
                    dataset1_records_to_create.append(dataset1records)
                    
                logger.info(f"{full_function_name}: Imported {len(batch_license_output_)} records (batch {i // batch_size + 1}).")
                logger.info(f"{full_function_name}: {dataset1_records_to_create}")
                if dataset1_records_to_create:
                    DataSet1Record.objects.bulk_create(dataset1_records_to_create)
                    logger.info(f"{full_function_name}: Imported {len(dataset1_records_to_create)} records.")
                    dataset1_records_to_create.clear()
                    time.sleep(2)
                # break
            logger.info(f"{full_function_name}: {message}")
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
    list_display = ("id","principalsInformation_entity_num","principalsInformation_entity_name","principalsInformation_first_name","principalsInformation_last_name","filling_information_file_status","principal_information_file_status","agentsInformation_file_status")
    search_fields = ['principalsInformation_entity_name']  # You can add other fields here as needed
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling, Principal & Agent and saved in the Dataset 2 (Combined Information)'
            batch_size = 10000
            fetch_principalsinformation = PrincipalsInformation.objects.all()
            total_principalsinformation_count = fetch_principalsinformation.count()
            logger.info(f"{full_function_name}: total number of Filings Information data: {total_principalsinformation_count}")
            # # Initialize a list to hold dataset2records instances for bulk create
            records_to_create = []
            for i in range(0, total_principalsinformation_count, batch_size):
                batch_principalsinformation = fetch_principalsinformation[i:i + batch_size]
                for principalsinformationdata in batch_principalsinformation:
                    entity_name = str(principalsinformationdata.principalsInformation_entity_name)
                    logger.info(f"{full_function_name}: entity_name = {entity_name}")
                    dataset2records = DataSet2Record()
                    agent_informations = AgentsInformation.objects.filter(agentsInformation_entity_name=entity_name).first()
                    filingsinformation = FilingsInformation.objects.filter(filingsInformation_entity_name=entity_name).first()
                    if agent_informations:
                        logger.info(f"{full_function_name}: Records Found in the Agents Information")
                        # If found in the Agents Information, copy the data to the dataset2records instance
                        for column_name in self.agents_information_all_columns:
                            if hasattr(agent_informations, column_name):
                                setattr(dataset2records, column_name, getattr(agent_informations, column_name))
                            dataset2records.agentsInformation_file_status = True
                    if filingsinformation:
                        for column_name in self.filings_Information_all_columns:
                            if hasattr(filingsinformation, column_name):
                                setattr(dataset2records, column_name, getattr(filingsinformation, column_name))
                            dataset2records.filling_information_file_status = True
                        logger.info(f"{full_function_name}: Records Found in the filings information")

                    logger.info(f"{full_function_name}: Records Found no found in the filings information")
                    for column_name in self.principalsinformation_all_columns:
                        if hasattr(principalsinformationdata, column_name):
                            setattr(dataset2records, column_name, getattr(principalsinformationdata, column_name))
                        dataset2records.principal_information_file_status = True
                    records_to_create.append(dataset2records)    
                logger.info(f"{full_function_name}: (records_to_create) = {records_to_create}")
                if dataset2records:
                    DataSet2Record.objects.bulk_create(records_to_create)
                    logger.info(f"{full_function_name}: Imported {len(records_to_create)} records (batch {i // batch_size + 1}).")
                    records_to_create.clear()
                    time.sleep(2)
                # break
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # Redirect after processing
        return DataSet2Recordmerge_view
@admin.register(DataErichment)
class DataErichmentAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "dataerichmentrecords"
    list_display = ("id","abc_business_name", "abc_licensee", "agentsInformation_entity_name", "data_set_1_file_status", "data_set_2_file_status")
    # search_fields = ['entity_name', 'licensee']  # Add more fields here if needed
    yelp_output_all_columns = get_column_names(YelpRestaurantRecord,['id'], include_relations=True)
    PrincipalsInformationCoulmne = get_column_names(PrincipalsInformation,['id'], include_relations=True)
    AgentsInformationCoulmne = get_column_names(AgentsInformation,['id'], include_relations=True)
    FilingsInformationCoulmne = get_column_names(FilingsInformation,['id'], include_relations=True)
    data_set_1_all_columns = get_column_names(DataSet1Record, ['id'], include_relations=True)
    data_set_2_all_columns = get_column_names(DataSet2Record, ['id'], include_relations=True)
    fieldsets = (
       ('ABC License Records', {
            'classes': ('collapse',),
            'fields': ('abc_license_number', 'abc_primary_owner', 'abc_office_of_application', 'abc_business_name', 'abc_business_address', 'abc_county', 'abc_census_tract', 'abc_licensee', 'abc_license_type', 'abc_license_type_status', 'abc_status_date', 'abc_term', 'abc_original_issue_date', 'abc_expiration_date', 'abc_master', 'abc_duplicate', 'abc_fee_code', 'abc_transfers', 'abc_conditions', 'abc_operating_restrictions', 'abc_disciplinary_action', 'abc_disciplinary_history', 'abc_holds', 'abc_escrows', 'abc_from_license_number', 'abc_transferred_on', 'abc_to_license_number', 'abc_transferred_on2'),
        }),('Google Scrapp Record', {
            'classes': ('collapse',''),
            'fields': ('google_business_name', 'google_business_address','google_place_name','google_rating','google_phone_number','google_website','google_types','google_business_status'),
        }),
         ('Yelp Restaurant Record', {
             'classes': ('collapse',''),
            'fields': tuple(yelp_output_all_columns),
        }),
        ('Principals Information Record', {
             'classes': ('collapse',''),
            'fields': tuple(PrincipalsInformationCoulmne),
        }),
        ('Agents Information Record', {
            'classes': ('collapse',''),
            'fields': tuple(AgentsInformationCoulmne),
        }),
         ('Filings Information Record', {
              'classes': ('collapse',''),
            'fields': tuple(FilingsInformationCoulmne),
        }),
          ('Another Information Record', {
              'classes': ('collapse',''),
            'fields': ('data_set_1_file_status','data_set_2_file_status'),
        }),
    )

    def get_merge_view(self):
        def DataSet3Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling , Principal & Agent and saved in Dataset 2 (Combined Information)'
            logger.info(f"{full_function_name}: {message}")
            batch_size = 100000
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
                    Value('.'), Value('')
                ),
                output_field=CharField()  # Correct usage of CharField as output field type
                 
                  ))
            normalized_second_table = DataSet2Record.objects.annotate(
                normalized_name=Lower(
                    Replace(
                        Replace(
                            Replace(
                                F('principalsInformation_entity_name'),
                                Value('|'), Value('')
                            ),
                            Value(','), Value('')
                        ),
                        Value('.'), Value('')
                    ),
                   output_field=CharField()  # Correct usage of CharField as output field type
                )
            )
            total_data_Set_1_count = normalized_first_table.count()
            logger.info(f"{full_function_name}: total number of data set 1 data: {total_data_Set_1_count}")   
            
            for i in range(0, total_data_Set_1_count, batch_size):
                batch = normalized_first_table[i:i + batch_size]
                for first_record in batch:
                    licensee = str(first_record.normalized_name)
                    logger.info(f"{full_function_name}: licensee = {licensee}")
                    matching_second_records = normalized_second_table.filter(normalized_name=licensee)
                    logger.info(f"{full_function_name}: matching_second_records = {matching_second_records}")
                    if matching_second_records:
                        logger.info(f"{full_function_name}: the licensee found in the dataset2 for any Records")
                        logger.info(f"{full_function_name}: matching_second_record = {len(matching_second_records)}")
                        for second_records in matching_second_records:
                            dataset3records = DataErichment()
                            for column_name in self.data_set_2_all_columns:
                                if hasattr(second_records, column_name):
                                    setattr(dataset3records, column_name, getattr(second_records, column_name))
                            dataset3records.data_set_2_file_status = True
                            for column_name in self.data_set_1_all_columns:
                                if hasattr(first_record, column_name):
                                    setattr(dataset3records, column_name, getattr(first_record, column_name))
                                dataset3records.data_set_1_file_status = True
                            dataset3records.save()
                    else:
                        logger.info(f"{full_function_name}: the licensee is not  found in the dataset2 for a Records")
                    status = 'created'
                    logger.info(f"{full_function_name}: {status} Data Set3 Record for license_number: {first_record.normalized_name}")
                logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                time.sleep(2)
                break
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataerichment/")
        return DataSet3Recordmerge_view     