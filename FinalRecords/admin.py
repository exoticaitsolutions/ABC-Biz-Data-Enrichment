import csv
from django.contrib import admin
from django.http import HttpResponseRedirect
from django.contrib import admin,messages
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import convert_to_int_str, get_column_names, get_full_function_name, normalize_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from FinalRecords.models import BusinessLocationLicense, EntityABCLicenseMapping, EntityAgentMapping, EntityPrincipalMapping, FilingAndAgentInfo, FilingAndPrincipalInfo
from core_app.models import AgentsInformation, FilingsInformation, PrincipalsInformation
from merge_data.models import DataSet1Record
from collections import defaultdict
from django.apps import apps
from ABC_BizEnrichment.common.logconfig import logger
import re
# Register your models here.

# Assuming CustomMergeAdminMixin and other necessary imports are done
@admin.register(BusinessLocationLicense)
class BusinessLocationLicenseAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "businesslocationlicense"
    list_display = ("id", "abc_license_number", "abc_licensee", "filingsInformation_entity_name", "dataset1_info", "filling_information_file_status")
    dataSet1_all_columns = get_column_names(DataSet1Record, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)

    def get_merge_view(self):
        def BusinessLocationLicensemerge_view(request):
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
                BusinessLocationLicense = apps.get_model('FinalRecords', 'BusinessLocationLicense')
                valid_fields = set(f.name for f in BusinessLocationLicense._meta.get_fields())
                # Collect data to insert in bulk
                to_create = []
                for lic in licensee_objects:
                    licensee_data = {
                        col: getattr(lic, col)
                        for col in self.dataSet1_all_columns
                        if hasattr(lic, col) and col in valid_fields
                    }
                    licensee_data['dataset1_info'] = status

                    filing_data = {
                        col: getattr(filing_obj, col)
                        for col in self.filings_Information_all_columns
                        if hasattr(filing_obj, col) and col in valid_fields
                    }
                    filing_data['filling_information_file_status'] = status
                    combined_data = {**filing_data, **licensee_data}
                    to_create.append(BusinessLocationLicense(**combined_data))
                # Bulk insert the records
                BusinessLocationLicense.objects.bulk_create(to_create)
            # Step 3: Use tqdm for progress bar
            for filing in tqdm(filings_qs, desc="Processing filings", unit="filing"):
                filing_name = filing.filingsInformation_entity_name
                normalized = normalize_name(filing_name)
                licensee_matches = normalized_licensee_map.get(normalized, [])
                if licensee_matches:
                    # insert_matched_records(filing, licensee_matches , True)
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
            print("\n--- Summary for the Filling and Data Set 1 ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Data Set 1  : {len(DataSet1Record.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the data set 1 : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the data set 1: {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_licensees}")
            message = 'Data Merged successfully for Filing, Principal & Agent and saved in BusinessLocationLicense.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/businesslocationlicense/")
        return BusinessLocationLicensemerge_view
    
@admin.register(EntityABCLicenseMapping)
class EntityABCLicenseMappingAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "entityabclicensemapping"
    list_display = ("id", "ABC_Licennse_ID_id", "Entity_Table_ID_id", "created_at", "updated_at")
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
                new_data = [
                    EntityABCLicenseMapping(
                        Entity_Table_ID=filing_obj,
                        ABC_Licennse_ID=lic
                    )
                    for lic in licensee_objects
                ]
                EntityABCLicenseMapping.objects.bulk_create(new_data, batch_size=500)
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
            print("\n--- Summary for the Filling and Data Set 1 ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Data Set 1  : {len(DataSet1Record.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the data set 1 : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the data set 1: {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_licensees}")
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
            principal_qs = PrincipalsInformation.objects.all()
            normalized_principal_map = defaultdict(list)
            for lic in principal_qs:
                normalized = lic.principalsInformation_entity_num
                normalized_principal_map[normalized].append(lic)
            filings_qs = FilingsInformation.objects.exclude(filingsInformation_entity_num__isnull=True).exclude(filingsInformation_entity_num__exact='')
            counter = 1
            matched_count = 0
            unmatched_count = 0
            total_matched_principal = 0
            unmatched_filings = []  # To store unmatched filings for CSV export
            def insert_matched__pricipal_records(filing_obj, principal_obj,status):
                FilingAndPrincipalInfo = apps.get_model('FinalRecords', 'FilingAndPrincipalInfo')
                valid_fields = set(f.name for f in FilingAndPrincipalInfo._meta.get_fields())
                to_create = []
                for principal in principal_obj:
                    principal_data = {
                        col: getattr(principal, col)
                        for col in self.principalsinformation_all_columns
                        if hasattr(principal, col) and col in valid_fields
                    }
                    principal_data['principal_information_file_status'] = status
                    filing_data = {
                        col: getattr(filing_obj, col)
                        for col in self.filings_Information_all_columns
                        if hasattr(filing_obj, col) and col in valid_fields
                    }
                    filing_data['filling_information_file_status'] = status
                    combined_data = {**filing_data, **principal_data}
                    to_create.append(FilingAndPrincipalInfo(**combined_data))
                FilingAndPrincipalInfo.objects.bulk_create(to_create)
            for filing in tqdm(filings_qs, desc="Processing filings and matched to the PrincipalsInformation ", unit="filing"):
                filing_enatity_num = filing.filingsInformation_entity_num
                principal_matches = normalized_principal_map.get(filing_enatity_num, [])
                # principal_matches = PrincipalsInformation.objects.filter(principalsInformation_entity_num=filing_enatity_num)
                if principal_matches:
                    insert_matched__pricipal_records(filing, principal_matches , True)
                    matched_count += 1
                    match_count = len(principal_matches)
                    total_matched_principal += match_count # ✅ Accumulate the count
                else:
                    unmatched_count += 1
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })
                counter += 1
                # break
            if unmatched_filings:
                with open('unmatched_filings_via_pricipal.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print(f"Unmatched filings saved to unmatched_filings.csv")
            print("\n--- Summary for the Filing And Principal Info ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Principal   : {len(PrincipalsInformation.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the Principals Information  : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the Principals Information : {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_principal}")
            message = 'Data Merged successfully for Filing, Principal and saved in FilingAndPrincipalInfo.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/filingandprincipalinfo/")
        return FilingAndPrincipalInfomerge_view
    

@admin.register(EntityPrincipalMapping)
class EntityPrincipalMappingAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "entityprincipalmapping"
    list_display = ("id", "Entity_Table_ID_id", "Principal_ID_id", "created_at", "updated_at")
    principalsinformation_all_columns = get_column_names(PrincipalsInformation, ['id'], include_relations=True)
    filings_Information_all_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    def get_merge_view(self):
        def entity_principal_mapping_merge_view(request):
            full_function_name = get_full_function_name()
            print('---------- Start Merge Process ------------------')
            print(len(FilingsInformation.objects.all()), "Total FilingsInformation")
            print(len(PrincipalsInformation.objects.all()), "Total PrincipalsInformation")

            principal_qs = PrincipalsInformation.objects.all()
            normalized_principal_map = defaultdict(list)

            for lic in principal_qs:
                normalized = lic.principalsInformation_entity_num
                normalized_principal_map[normalized].append(lic)

            filings_qs = FilingsInformation.objects.exclude(
                filingsInformation_entity_num__isnull=True
            ).exclude(
                filingsInformation_entity_num__exact=''
            )

            matched_count = 0
            unmatched_count = 0
            total_matched_principal = 0
            unmatched_filings = []

            def insert_matched_principal_records(filing_obj, principal_objs):
                new_data = [
                    EntityPrincipalMapping(
                        Entity_Table_ID=filing_obj,
                        Principal_ID=principal
                    )
                    for principal in principal_objs
                ]
                EntityPrincipalMapping.objects.bulk_create(new_data, batch_size=500)

            for filing in tqdm(filings_qs, desc="Matching Filings to Principals", unit="filing"):
                filing_entity_num = filing.filingsInformation_entity_num
                principal_matches = normalized_principal_map.get(filing_entity_num, [])

                if principal_matches:
                    insert_matched_principal_records(filing, principal_matches)
                    matched_count += 1
                    total_matched_principal += len(principal_matches)
                else:
                    unmatched_count += 1
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })

            if unmatched_filings:
                with open(f'EntityPrincipalMapping.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print("❌ Unmatched filings saved to EntityPrincipalMapping.csv")

            print("\n--- Summary for the Filing And Principal Info ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Principal   : {len(PrincipalsInformation.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the Principals Information  : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the Principals Information : {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_principal}")

            message = 'Data merged successfully for Filings and Principals and saved in EntityPrincipalMapping.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/entityprincipalmapping/")

        return entity_principal_mapping_merge_view


@admin.register(FilingAndAgentInfo)
class FilingAndAgentInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "filingandagentinfo"
    list_display = ("id", "filingsInformation_entity_num", "agentsInformation_entity_num","filingsInformation_entity_name","agentsInformation_entity_name", "filling_information_file_status", "agent_information_file_status")
    agent_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    def get_merge_view(self):
        def FilingAndAgentInfomerge_view(request):
            print('----------start------------------')
            print(len(FilingsInformation.objects.all()), "Filings total")
            print(len(AgentsInformation.objects.all()), "Licensee total")
            agent_qs = AgentsInformation.objects.all()
            normalized_agent_map = defaultdict(list)
            for agent in agent_qs:
                normalized = agent.agentsInformation_entity_num
                normalized_agent_map[normalized].append(agent)
            filings_qs = FilingsInformation.objects.all()
            matched_count = 0
            unmatched_count = 0
            total_matched_principal = 0
            unmatched_filings = []
            def insert_matched_agent_records(filing_obj, agent_objs):
                FilingAndAgentInfo = apps.get_model('FinalRecords', 'FilingAndAgentInfo')
                valid_fields = set(f.name for f in FilingAndAgentInfo._meta.get_fields())
                to_create = []
                for agent in agent_objs:
                    agent_data = {
                        col: getattr(agent, col)
                        for col in self.agent_columns
                        if hasattr(agent, col) and col in valid_fields
                    }
                    agent_data['agent_information_file_status'] = True
                    filing_data = {
                        col: getattr(filing_obj, col)
                        for col in self.filings_columns
                        if hasattr(filing_obj, col) and col in valid_fields
                    }
                    filing_data['filling_information_file_status'] = True
                    combined_data = {**filing_data, **agent_data}
                    to_create.append(FilingAndAgentInfo(**combined_data))
                FilingAndAgentInfo.objects.bulk_create(to_create)
            for filing in tqdm(filings_qs, desc="Matching Filings to Agents", unit="filing"):
                filing_entity_num = convert_to_int_str(filing.filingsInformation_entity_num)
                agent_matches = normalized_agent_map.get(filing_entity_num, [])
                if agent_matches:
                    insert_matched_agent_records(filing, agent_matches)
                    matched_count += 1
                    match_count = len(agent_matches)
                    total_matched_principal += match_count
                else:
                    unmatched_count += 1
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })
            
            if unmatched_filings:
                with open(f'FilingAndAgentInfo.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print("❌ Unmatched filings saved to FilingAndAgentInfo.csv")
            print("\n--- Summary for the Filing And Agent Info ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Agent   : {len(AgentsInformation.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the Agent Information  : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the Agent Information : {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_principal}")
            message = 'Data Merged successfully for Filing with  the Agent in the Filing And AgentInfo'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/filingandagentinfo/")
        return FilingAndAgentInfomerge_view
    

@admin.register(EntityAgentMapping)
class EntityAgentMappingAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "entityagentmapping"
    list_display = ("id", "Entity_Table_ID_id", "Agent_ID","created_at","updated_at")
    agent_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)
    def get_merge_view(self):
        def EntityAgentMappingmerge_view(request):
            print('----------start------------------')
            print(len(FilingsInformation.objects.all()), "Filings total")
            print(len(AgentsInformation.objects.all()), "Licensee total")
            agent_qs = AgentsInformation.objects.all()
            normalized_agent_map = defaultdict(list)
            for agent in agent_qs:
                normalized = agent.agentsInformation_entity_num
                normalized_agent_map[normalized].append(agent)
            filings_qs = FilingsInformation.objects.all()
            matched_count = 0
            unmatched_count = 0
            total_matched_principal = 0
            unmatched_filings = []
            def insert_matched_agent_records(filing_obj, agent_objs):
                new_data = [
                    EntityAgentMapping(
                        Entity_Table_ID=filing_obj,
                        Agent_ID=lic
                    )
                    for lic in agent_objs
                ]
                EntityAgentMapping.objects.bulk_create(new_data, batch_size=500)
            for filing in tqdm(filings_qs, desc="Matching Filings to Agents", unit="filing"):
                filing_entity_num = convert_to_int_str(filing.filingsInformation_entity_num)
                agent_matches = normalized_agent_map.get(filing_entity_num, [])
                if agent_matches:
                    insert_matched_agent_records(filing, agent_matches)
                    matched_count += 1
                    match_count = len(agent_matches)
                    total_matched_principal += match_count
                else:
                    unmatched_count += 1
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })
            
            if unmatched_filings:
                with open(f'FilingAndAgentInfo.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print("❌ Unmatched filings saved to FilingAndAgentInfo.csv")
            print("\n--- Summary for the Filing And Agent Info ---")
            print(f'Total Number of the Records In the Filling : {len(FilingsInformation.objects.all())}')
            print(f'Total Number of the Records In the Agent   : {len(AgentsInformation.objects.all())}')
            print(f"✅ Total Matched Filings search on teh based of the Agent Information  : {matched_count}")
            print(f"❌ Total Matched Filings search on teh based of the Agent Information : {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found and insert int the new Table : {total_matched_principal}")
            message = 'Data Merged successfully for Filing with  the Agent in the Filing And AgentInfo'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/entityagentmapping/")
        return EntityAgentMappingmerge_view