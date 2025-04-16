import csv
from django.contrib import admin
from django.http import HttpResponseRedirect
from django.contrib import admin,messages
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import convert_to_int_str, get_column_names, get_full_function_name, normalize_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from FinalRecords.models import BusinessLocationLicense, EntityABCLicenseMapping, FilingAndPrincipalInfo
from core_app.models import FilingsInformation, PrincipalsInformation
from merge_data.models import DataSet1Record
from collections import defaultdict
from django.apps import apps
from ABC_BizEnrichment.common.logconfig import logger
import re
# Register your models here.

# Assuming CustomMergeAdminMixin and other necessary imports are done
# @admin.register(BusinessLocationLicense)
# class BusinessLocationLicenseAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
#     merge_url_name = "businesslocationlicense"
#     list_display = ("id", "abc_license_number", "abc_licensee", "filingsInformation_entity_name", "dataset1_info", "filling_information_file_status")
#     dataSet1_all_columns = get_column_names(DataSet1Record, ['id'], include_relations=True)
#     filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)

#     def get_merge_view(self):
#         def BusinessLocationLicensemerge_view(request):
#             print('----------start------------------')
#             print(len(FilingsInformation.objects.all()), "Filings total")
#             print(len(DataSet1Record.objects.all()), "Licensee total")
#             # Step 1: Build normalized licensee name map with full objects
#             licensee_qs = DataSet1Record.objects.exclude(abc_licensee__isnull=True).exclude(abc_licensee__exact='')
#             normalized_licensee_map = defaultdict(list)

#             for lic in licensee_qs:
#                 normalized = normalize_name(lic.abc_licensee)
#                 normalized_licensee_map[normalized].append(lic)

#             filings_qs = FilingsInformation.objects.exclude(filingsInformation_entity_name__isnull=True).exclude(filingsInformation_entity_name__exact='')
#             counter = 1
#             matched_count = 0
#             unmatched_count = 0
#             total_matched_licensees = 0
#             unmatched_filings = []  # To store unmatched filings for CSV export
            
#             # Step 2: Use bulk_create for batch insertion
#             def insert_matched_records(filing_obj, licensee_objects,status):
#                 BusinessLocationLicense = apps.get_model('FinalRecords', 'BusinessLocationLicense')
#                 valid_fields = set(f.name for f in BusinessLocationLicense._meta.get_fields())
#                 # Collect data to insert in bulk
#                 to_create = []
#                 for lic in licensee_objects:
#                     licensee_data = {
#                         col: getattr(lic, col)
#                         for col in self.dataSet1_all_columns
#                         if hasattr(lic, col) and col in valid_fields
#                     }
#                     licensee_data['dataset1_info'] = status

#                     filing_data = {
#                         col: getattr(filing_obj, col)
#                         for col in self.filings_Information_all_columns
#                         if hasattr(filing_obj, col) and col in valid_fields
#                     }
#                     filing_data['filling_information_file_status'] = status
#                     combined_data = {**filing_data, **licensee_data}
#                     to_create.append(BusinessLocationLicense(**combined_data))
#                 # Bulk insert the records
#                 BusinessLocationLicense.objects.bulk_create(to_create)
#             # Step 3: Use tqdm for progress bar
#             for filing in tqdm(filings_qs, desc="Processing filings", unit="filing"):
#                 filing_name = filing.filingsInformation_entity_name
#                 normalized = normalize_name(filing_name)
#                 licensee_matches = normalized_licensee_map.get(normalized, [])
#                 if licensee_matches:
#                     insert_matched_records(filing, licensee_matches , True)
#                     matched_count += 1
#                     match_count = len(licensee_matches)
#                     total_matched_licensees += match_count # ✅ Accumulate the count
#                 else:
#                     unmatched_count += 1
#                     # insert_matched_records(filing, licensee_matches , False)
#                     unmatched_filings.append({
#                         'filing_id': filing.id,
#                         'filings_entity_num': filing.filingsInformation_entity_num,
#                         'filing_name': filing.filingsInformation_entity_name,
#                         'status': 'Unmatched'
#                     })
#                 counter += 1
#             # Step 4: Summary
#             if unmatched_filings:
#                 with open('unmatched_filings.csv', mode='w', newline='', encoding='utf-8') as file:
#                     writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
#                     writer.writeheader()
#                     writer.writerows(unmatched_filings)
#                 print(f"Unmatched filings saved to unmatched_filings.csv")
#             print("\n--- Summary ---")
#             print(f"✅ Total Matched Filings: {matched_count}")
#             print(f"❌ Total Unmatched Filings: {unmatched_count}")
#             print(f"📦 Total Processed Filings: {matched_count + unmatched_count}")
#             print(f"🔢 Total Licensee Matches Found: {total_matched_licensees}")
#             message = 'Data Merged successfully for Filing, Principal & Agent and saved in BusinessLocationLicense.'
#             self.message_user(request, message, messages.SUCCESS)
#             return HttpResponseRedirect("/admin/FinalRecords/businesslocationlicense/")
#         return BusinessLocationLicensemerge_view
    
@admin.register(EntityABCLicenseMapping)
class EntityABCLicenseMappingAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "entityabclicensemapping"
    list_display = ("id", "Entity_Table_ID", "ABC_Licennse_ID")
    dataSet1_all_columns = get_column_names(DataSet1Record, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    def get_merge_view(self):
        def EntityABCLicenseMappingmerge_view(request):
            print('----------start------------------')
            print(len(FilingsInformation.objects.all()), "Filings total")
            print(len(DataSet1Record.objects.all()), "Licensee total")
            # Step 1: Build normalized licensee name map with full objects
            licensee_qs = DataSet1Record.objects.exclude(abc_licensee__isnull=True).exclude(abc_licensee__exact='')
            normalized_licensee_map = defaultdict(list)

            for lic in licensee_qs:
                normalized = normalize_name(lic.abc_licensee)
                normalized_licensee_map[normalized].append(lic)

            filings_qs = FilingsInformation.objects.exclude(filingsInformation_entity_name__isnull=True).exclude(filingsInformation_entity_name__exact='')
            counter = 1
            matched_count = 0
            unmatched_count = 0
            total_matched_licensees = 0
            unmatched_filings = []  # To store unmatched filings for CSV export
            
            # Step 2: Use bulk_create for batch insertion
            def insert_matched_records(filing_obj, licensee_objects,status):
                new_data = EntityABCLicenseMapping()
                # # Collect data to insert in bulk
                for lic in licensee_objects:
                    new_data.Entity_Table_ID = filing_obj
                    new_data.ABC_Licennse_ID = lic
                    new_data.save()
            # Step 3: Use tqdm for progress bar
            for filing in tqdm(filings_qs, desc="Processing filings", unit="filing"):
                filing_name = filing.filingsInformation_entity_name
                normalized = normalize_name(filing_name)
                licensee_matches = normalized_licensee_map.get(normalized, [])
                if licensee_matches:
                    insert_matched_records(filing, licensee_matches , True)
                    matched_count += 1
                    match_count = len(licensee_matches)
                    total_matched_licensees += match_count # ✅ Accumulate the count
                else:
                    unmatched_count += 1
                    # insert_matched_records(filing, licensee_matches , False)
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })
                counter += 1
            # Step 4: Summary
            if unmatched_filings:
                with open('unmatched_filings.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print(f"Unmatched filings saved to unmatched_filings.csv")
            print("\n--- Summary ---")
            print(f"✅ Total Matched Filings: {matched_count}")
            print(f"❌ Total Unmatched Filings: {unmatched_count}")
            print(f"📦 Total Processed Filings: {matched_count + unmatched_count}")
            print(f"🔢 Total Licensee Matches Found: {total_matched_licensees}")
            message = 'Data Merged successfully for Filing, Principal & Agent and saved in BusinessLocationLicense.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/entityabclicensemapping/")
        return EntityABCLicenseMappingmerge_view
    


@admin.register(FilingAndPrincipalInfo)
class FilingAndPrincipalInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "filingandprincipalinfo"
    list_display = ("id", "filingsInformation_entity_num", "principalsInformation_entity_num",  "principal_information_file_status", "filling_information_file_status")
    principalsinformation_all_columns = get_column_names(PrincipalsInformation, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    def get_merge_view(self):
        def FilingAndPrincipalInfomerge_view(request):
            full_function_name = get_full_function_name()
            print('----------start------------------')
            print(len(FilingsInformation.objects.all()), "FilingsInformation total")
            print(len(PrincipalsInformation.objects.all()), "PrincipalsInformation total")
            fetch_filingsinformation = FilingsInformation.objects.exclude(filingsInformation_entity_num__isnull=True).exclude(filingsInformation_entity_num__exact='')
            total_filingsinformation_count = fetch_filingsinformation.count()
            logger.debug(f"{full_function_name}: total number of Filings Information data: {total_filingsinformation_count}")
            principal_info_dict = {fi.principalsInformation_entity_num: fi for fi in PrincipalsInformation.objects.all()}
            records_to_create = []
            for filingsinformationdata in tqdm(fetch_filingsinformation, desc="Merging Data", total=total_filingsinformation_count, unit="record"):
                entity_number = str(filingsinformationdata.filingsInformation_entity_num)
                principalsinformation = principal_info_dict.get(entity_number)
                dataset2records = FilingAndPrincipalInfo()
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
                    if len(records_to_create) >= 500:
                        FilingAndPrincipalInfo.objects.bulk_create(records_to_create)
                        records_to_create.clear()
            if records_to_create:
                FilingAndPrincipalInfo.objects.bulk_create(records_to_create)
                records_to_create.clear()
            print("\n--- Summary ---")
            print(f"✅ Total Matched Filings: {len(records_to_create)}")
            print(f"❌ Total Unmatched Filings: {total_filingsinformation_count - len(records_to_create)}")
            print(f"📦 Total Processed Filings: {total_filingsinformation_count}")
            print(f"🔢 Total Licensee Matches Found: {len(records_to_create)}")
            message = 'Data Merged successfully for Filing, Principal and saved in FilingAndPrincipalInfo.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/filingandprincipalinfo/")
        return FilingAndPrincipalInfomerge_view
        