
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
                    logger.debug(f'Found == {license_number}')
                    for column_nameq in self.license_output_all_columns:
                        if hasattr(yelp_output, column_nameq):
                            setattr(dataset1records, column_nameq, getattr(yelp_output, column_nameq))
                    dataset1records.output_license_file_status = True
                    
                    for column_name in self.yelp_output_all_columns:
                        if hasattr(batch_license_output_data, column_name):
                            setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                    dataset1records.yelp_file_status = True
                else:
                    logger.debug(f'Not Found == {license_number}')
                    for column_name in self.yelp_output_all_columns:
                            if hasattr(batch_license_output_data, column_name):
                                setattr(dataset1records, column_name, getattr(batch_license_output_data, column_name))
                    dataset1records.output_license_file_status = False
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
    def get_merge_view(self):
        def DataSet2Recordmerge_view(request):
            print("------match the records --------------")
            
            dataset1_records = DataSet1Record.objects.all()
            
            count = 0

            # for record in dataset1_records:
            #     license_number = record.abc_license_number

            #     filing = FilingsInformation.objects.filter(filingsInformation_entity_num=license_number).first()
            #     agent = AgentsInformation.objects.filter(agentsInformation_entity_num=license_number).first()
            #     principal = PrincipalsInformation.objects.filter(principalsInformation_entity_num=license_number).first()

            #     if any([filing, agent, principal]):
            #         ds2 = DataSet2Record()

            #         if agent:
            #             ds2.agentsInformation_entity_name = agent.agentsInformation_entity_name
            #             ds2.agentsInformation_entity_num = agent.agentsInformation_entity_num
            #             ds2.agentsInformation_org_name = getattr(agent, 'agentsInformation_org_name', "")
            #             ds2.agentsInformation_first_name = getattr(agent, 'agentsInformation_first_name', "")
            #             ds2.agentsInformation_middle_name = getattr(agent, 'agentsInformation_middle_name', "")
            #             ds2.agentsInformation_last_name = getattr(agent, 'agentsInformation_last_name', "")
            #             ds2.agentsInformation_physical_address1 = getattr(agent, 'agentsInformation_physical_address1', "")
            #             ds2.agentsInformation_physical_address2 = getattr(agent, 'agentsInformation_physical_address2', "")
            #             ds2.agentsInformation_physical_address3 = getattr(agent, 'agentsInformation_physical_address3', "")
            #             ds2.agentsInformation_physical_city = getattr(agent, 'agentsInformation_physical_city', "")
            #             ds2.agentsInformation_physical_state = getattr(agent, 'agentsInformation_physical_state', "")
            #             ds2.agentsInformation_physical_country = getattr(agent, 'agentsInformation_physical_country', "")
            #             ds2.agentsInformation_physical_postal_code = getattr(agent, 'agentsInformation_physical_postal_code', "")
            #             ds2.agentsInformation_agent_type = getattr(agent, 'agentsInformation_agent_type', "")
            #             ds2.agentsInformation_file_status = True

            #         if filing:
            #             for field in [f.name for f in FilingsInformation._meta.fields]:
            #                 if hasattr(ds2, field):
            #                     setattr(ds2, field, getattr(filing, field))
            #             ds2.filling_information_file_status = True

            #         if principal:
            #             for field in [f.name for f in PrincipalsInformation._meta.fields]:
            #                 if hasattr(ds2, field):
            #                     setattr(ds2, field, getattr(principal, field))
            #             ds2.principal_information_file_status = True

            #         ds2.save()
            #         count += 1
            
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