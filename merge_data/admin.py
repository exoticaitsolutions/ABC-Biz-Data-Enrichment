
import logging
from datetime import datetime
import os
import time
from django.contrib import admin,messages
from django.conf import settings
from ABC_BizEnrichment.common.helper_function import get_column_names, get_full_function_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from django.http import HttpResponseRedirect 
from core_app.models import AgentsInformation, FilingsInformation, LicenseOutput, PrincipalsInformation, YelpRestaurantRecord
from merge_data.models import DataSet1Record, DataSet2Record 
# Logger setup
from import_export.admin import ExportMixin, ImportExportModelAdmin
LOG_FOLDER = f"storage/logs/{datetime.now().strftime('%Y%m%d')}"
os.makedirs(
    LOG_FOLDER, exist_ok=True
)  #
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(f"{LOG_FOLDER}/merge_app_log_{datetime.now().strftime('%Y%m%d')}.log")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

@admin.register(DataSet1Record)
class DataSet1RecordAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "export_data_set1"
    actions = ["merge_and_import_data_action"]
    search_fields = ['licensee']  # You can add other fields here as needed
    list_display = ("id", "file_number","license_number","business_name","primary_owner","primary_name","dba_name","output_license_file_status","yelp_file_status")
    def get_merge_view(self):
        def MergeDataSet1RecordsAdmin(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for LicenseOutput & AbcBizYelpRestaurantData and saved in the Dataset 1 (Business License)'
            # Get all column names (including relation fields)
            license_output_all_columns = get_column_names(LicenseOutput,['id', 'license_number'], include_relations=True)
            yelp_output_all_columns = get_column_names(YelpRestaurantRecord,['id', 'file_number'], include_relations=True)
            batch_size = 10000
            license_output_qs = LicenseOutput.objects.all()
            total_license_count = license_output_qs.count()
            logger.info(f"{full_function_name}: total number of Data Set1 Record data: {total_license_count}")
            # Process in batches
            for i in range(0, total_license_count, batch_size):
                batch = license_output_qs[i:i+batch_size]  # Get a slice of the queryset
                for license_output_data in batch:
                    yelp_output = YelpRestaurantRecord.objects.filter(file_number=license_output_data.license_number).first()
                    dataset1records, created = DataSet1Record.objects.get_or_create(license_number=str(license_output_data.license_number))
                    if yelp_output:
                        print('yelp_output records  found')
                        for column_name in yelp_output_all_columns:
                            if hasattr(yelp_output, column_name):
                                setattr(dataset1records, column_name, getattr(yelp_output, column_name))
                        dataset1records.file_number = yelp_output.file_number
                        dataset1records.yelp_file_status = True
                    for column_name in license_output_all_columns:
                        if hasattr(license_output_data, column_name):
                            setattr(dataset1records, column_name, getattr(license_output_data, column_name))
                    dataset1records.output_license_file_status = True
                    dataset1records.save()
                    if created:
                        action = "Created"
                    else:
                        action = "Fetched"
                    logger.info(f"{full_function_name}: {action} BusinessLicense for license_number: {dataset1records.license_number}")
                logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                time.sleep(10)
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset1record/")  # This 
        return MergeDataSet1RecordsAdmin



@admin.register(DataSet2Record)
class DataSet2RecordAdmin(CustomMergeAdminMixin,ExportMixin, admin.ModelAdmin):
    merge_url_name = "data_set2_records"
    list_display = ("id","entity_num","entity_name","filling_information_file_status","principal_information_file_status","agent_information_file_status")
    search_fields = ['entity_name']  # You can add other fields here as needed
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            full_function_name = get_full_function_name()
            message = 'Data Merged successfully for Filling , Principal & Agent  and saved in the Dataset 2 (Combined Information)'
            agents_information_all_columns = get_column_names(AgentsInformation,['id', 'entity_num','org_name','entity_name'], include_relations=True)
            logger.info(f"{full_function_name}: agents_information_all_columns here {agents_information_all_columns}")
            filings_Information_all_columns = get_column_names(FilingsInformation,['id', 'entity_num','principal_address','principal_address2','principal_city','principal_state','principal_country','principal_postal_code','principal_address_in_ca','principal_address2_in_ca','principal_city_in_ca','principal_state_in_ca','principal_country_in_ca','principal_postal_code_in_ca'], include_relations=True)
            logger.info(f"{full_function_name}: filings_Information_all_columns here {filings_Information_all_columns}")
            principalsinformation_all_columns = get_column_names(PrincipalsInformation,['id', 'entity_num','entity_name','principal_address','principal_address2','principal_city','principal_state','principal_country','principal_postal_code','principal_address_in_ca','principal_address2_in_ca','principal_city_in_ca','principal_state_in_ca','principal_country_in_ca','principal_postal_code_in_ca'], include_relations=True)
            logger.info(f"{full_function_name}: filings_Information_all_columns here {principalsinformation_all_columns}")
            batch_size = getattr(settings, 'BATCH_SIZE', 1000)
            fetch_filings_information = FilingsInformation.objects.all()
            total_filings_information_count = fetch_filings_information.count()
            logger.info(f"{full_function_name}: total number of Filings Information data: {total_filings_information_count}")
            for i in range(0, total_filings_information_count, batch_size):
                batch = fetch_filings_information[i:i + batch_size]  # Fetch the current batch
                for filling_info_data in batch:
                    entity_number = str(filling_info_data.entity_num)
                    print('entity_number',entity_number)
                    dataset2records, created = DataSet2Record.objects.get_or_create(entity_num=entity_number)
                    agent_informations = AgentsInformation.objects.filter(entity_num=entity_number).first()
                    principals_information = PrincipalsInformation.objects.filter(entity_num=entity_number).first()
                    if agent_informations:
                        print('Records Found in the Agents Information')
                        for column_name in agents_information_all_columns:
                            if hasattr(agent_informations, column_name):
                                setattr(dataset2records, column_name, getattr(agent_informations, column_name))
                        dataset2records.agent_information_org_name = agent_informations.org_name
                        dataset2records.agent_information_entity_name = agent_informations.entity_name
                        dataset2records.agent_information_file_status = True        
                    if principals_information:
                        print('Records Found in the principals information')
                        for column_name in principalsinformation_all_columns:
                            if hasattr(principals_information, column_name):
                                setattr(dataset2records, column_name, getattr(principals_information, column_name))
                        dataset2records.principal_information_principal_address = principals_information.principal_address
                        dataset2records.principal_information_principal_address2 = principals_information.principal_address2
                        dataset2records.principal_information_principal_city = principals_information.principal_city
                        dataset2records.principal_information_principal_state = principals_information.principal_state
                        dataset2records.principal_information_principal_country = principals_information.principal_country
                        dataset2records.principal_information_principal_postal_code = principals_information.principal_postal_code
                        dataset2records.principal_information_principal_address_in_ca = principals_information.principal_address_in_ca
                        dataset2records.principal_information_principal_address2_in_ca = principals_information.principal_address2_in_ca
                        dataset2records.principal_information_principal_city_in_ca = principals_information.principal_city_in_ca
                        dataset2records.principal_information_principal_state_in_ca = principals_information.principal_state_in_ca
                        dataset2records.principal_information_principal_country_in_ca = principals_information.principal_country_in_ca
                        dataset2records.principal_information_principal_postal_code_in_ca = principals_information.principal_postal_code_in_ca
                        dataset2records.principal_information_file_status = True
                    # common 
                    for column_name in filings_Information_all_columns:
                        if hasattr(filling_info_data, column_name):
                            setattr(dataset2records, column_name, getattr(filling_info_data, column_name))
                    dataset2records.filling_information_principal_address = filling_info_data.principal_address
                    dataset2records.filling_information_principal_address2 = filling_info_data.principal_address2
                    dataset2records.filling_information_principal_city = filling_info_data.principal_city
                    dataset2records.filling_information_principal_state = filling_info_data.principal_state
                    dataset2records.filling_information_principal_country = filling_info_data.principal_country
                    dataset2records.filling_information_principal_postal_code = filling_info_data.principal_postal_code
                    dataset2records.filling_information_principal_address_in_ca = filling_info_data.principal_address_in_ca
                    dataset2records.filling_information_principal_address2_in_ca = filling_info_data.principal_address2_in_ca
                    dataset2records.filling_information_principal_city_in_ca = filling_info_data.principal_city_in_ca
                    dataset2records.filling_information_principal_state_in_ca = filling_info_data.principal_state_in_ca
                    dataset2records.filling_information_principal_country_in_ca = filling_info_data.principal_country_in_ca
                    dataset2records.filling_information_principal_postal_code_in_ca = filling_info_data.principal_postal_code_in_ca
                    dataset2records.filling_information_file_status = True
                    dataset2records.save()
                    if created:
                        action = "Created"
                    else:
                        action = "Fetched"
                    logger.info(f"{full_function_name}: {action} Data Set2 Record for license_number: {dataset2records.entity_num}")
                logger.info(f"{full_function_name}: Imported {len(batch)} records (batch {i // batch_size + 1}).")
                time.sleep(10)
            logger.info(f"{full_function_name}: {message}")
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/merge_data/dataset2record/")  # This 
        return DataSet2Recordmerge_view