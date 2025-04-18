from concurrent.futures import ThreadPoolExecutor
import csv
import os
from django.db import transaction
from django.contrib import admin
from django.http import HttpResponseRedirect
from django.contrib import admin,messages
import pandas as pd
from tqdm import tqdm
from ABC_BizEnrichment.common.helper_function import get_column_names, get_full_function_name, normalize_name
from ABC_BizEnrichment.common.merge_data.helper_function import CustomMergeAdminMixin
from FinalRecords.models import BusinessLocationLicense, EntityABCLicenseMapping, EntityAgentMapping, EntityPrincipalMapping, FilingAndAgentInfo, FilingAndPrincipalInfo
from core_app.models import AgentsInformation, FilingsInformation, PrincipalsInformation
from merge_data.models import DataSet1Record
from collections import defaultdict
from django.apps import apps
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
            
            filings_qs = FilingsInformation.objects.exclude(filingsInformation_entity_num__isnull=True).exclude(filingsInformation_entity_num__exact='')
            unique_filings_dict = {}
            for filing in filings_qs:
                try:
                    normalized_num = int(float(filing.filingsInformation_entity_num))
                    if normalized_num not in unique_filings_dict:
                        unique_filings_dict[normalized_num] = filing
                except ValueError:
                    continue
            unique_filings = list(unique_filings_dict.values())

            matched_count = 0
            unmatched_count = 0
            total_matched_licensees = 0
            unmatched_filings = []  # To store unmatched filings for CSV export

            def insert_matched_records(filing_obj, licensee_objects, status):
                BusinessLocationLicense = apps.get_model('FinalRecords', 'BusinessLocationLicense')
                valid_fields = set(f.name for f in BusinessLocationLicense._meta.get_fields())
                to_create = []
                new_data = [
                    EntityABCLicenseMapping(
                        Entity_Table_ID=filing_obj,
                        ABC_Licennse_ID=lic
                    )
                    for lic in licensee_objects
                ]
                EntityABCLicenseMapping.objects.bulk_create(new_data, batch_size=100)
                
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
                
                # Bulk insert in batches
                with transaction.atomic():
                    for i in range(0, len(to_create), 100):
                        BusinessLocationLicense.objects.bulk_create(to_create[i:i+100])

            # Step 2: Parallel Processing with ThreadPoolExecutor
            def match_filing(filing):
                nonlocal matched_count, unmatched_count, total_matched_licensees
                filing_name = filing.filingsInformation_entity_name
                normalized = normalize_name(filing_name)
                licensee_matches = normalized_licensee_map.get(normalized, [])
                
                if licensee_matches:
                    insert_matched_records(filing, licensee_matches, True)
                    matched_count += 1
                    total_matched_licensees += len(licensee_matches)
                else:
                    unmatched_count += 1
                    unmatched_filings.append({
                        'filing_id': filing.id,
                        'filings_entity_num': filing.filingsInformation_entity_num,
                        'filing_name': filing.filingsInformation_entity_name,
                        'status': 'Unmatched'
                    })

            # Using ThreadPoolExecutor for parallel matching
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(tqdm(executor.map(match_filing, unique_filings), total=len(unique_filings), desc="Matching filings"))

            # Step 3: Export unmatched filings
            if unmatched_filings:
                with open('unmatched_filings.csv', mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=['filing_id', 'filings_entity_num', 'filing_name', 'status'])
                    writer.writeheader()
                    writer.writerows(unmatched_filings)
                print(f"Unmatched filings saved to unmatched_filings.csv")

            # Summary Output
            print(f"\n--- Summary ---")
            print(f"🧾 Total Unique Filings: {len(unique_filings)}")
            print(f"✅ Total Matched: {matched_count}")
            print(f"❌ Total Unmatched: {unmatched_count}")
            print(f"🔢 Total Licensee Matches Found: {total_matched_licensees}")
            
            message = 'Data Merged successfully for Filing, Principal & Agent and saved in BusinessLocationLicense.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/businesslocationlicense/")
        
        return BusinessLocationLicensemerge_view
    

    


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
                normalized = float(lic.principalsInformation_entity_num)
                normalized_principal_map[normalized].append(lic)
            filings_qs = FilingsInformation.objects.exclude(filingsInformation_entity_num__isnull=True).exclude(filingsInformation_entity_num__exact='')
            unique_filings_dict = {}
            for filing in filings_qs:
                try:
                    normalized_num = int(float(filing.filingsInformation_entity_num))
                    if normalized_num not in unique_filings_dict:
                        unique_filings_dict[normalized_num] = filing
                except ValueError:
                    continue
            unique_filings = list(unique_filings_dict.values())
            counter = 1
            matched_count = 0
            unmatched_count = 0
            total_matched_principal = 0
            unmatched_filings = []  # To store unmatched filings for CSV export
            def insert_matched__pricipal_records(filing_obj, principal_obj,status):
                FilingAndPrincipalInfo = apps.get_model('FinalRecords', 'FilingAndPrincipalInfo')
                valid_fields = set(f.name for f in FilingAndPrincipalInfo._meta.get_fields())
                to_create = []
                new_data = [
                    EntityPrincipalMapping(
                        Entity_Table_ID=filing_obj,
                        Principal_ID=principal
                    )
                    for principal in principal_obj
                ]
                EntityPrincipalMapping.objects.bulk_create(new_data, batch_size=500)
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
            for filing in tqdm(unique_filings, desc="Processing filings and matched to the PrincipalsInformation ", unit="filing"):
                try:
                    filing_float = float(filing.filingsInformation_entity_num)
                except ValueError:
                    continue
                principal_matches = normalized_principal_map.get(filing_float, [])
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
            print("\n--- Summary of Filing and Principal Information ---")
            print(f"🧾 Total Unique Filings: {len(unique_filings)}")
            print(f'Total number of records in Principals: {len(PrincipalsInformation.objects.all())}')
            print(f"✅ Total matched filings based on Principals Information: {matched_count}")
            print(f"❌ Total unmatched filings based on Principals Information: {unmatched_count}")
            print(f"🔢 Total principal matches found and inserted into the new table: {total_matched_principal}")
            message = 'Data Merged successfully for Filing, Principal and saved in FilingAndPrincipalInfo.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/filingandprincipalinfo/")
        return FilingAndPrincipalInfomerge_view
    

@admin.register(FilingAndAgentInfo)
class FilingAndAgentInfoAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "filingandagentinfo"
    list_display = ("id", "filingsInformation_entity_num", "agentsInformation_entity_num",
                    "filingsInformation_entity_name", "agentsInformation_entity_name", 
                    "filling_information_file_status", "agent_information_file_status")
    agent_columns = get_column_names(AgentsInformation, ['id'], include_relations=True)
    filings_columns = get_column_names(FilingsInformation, ['id'], include_relations=True)

    def get_merge_view(self):
        def FilingAndAgentInfomerge_view(request):
            print('---------- Start Agent Merge Process ------------------')
            print(f"{FilingsInformation.objects.count()} Total FilingsInformation")
            print(f"{AgentsInformation.objects.count()} Total AgentsInformation")

            # Step 1: Build a map of agent entity numbers (integer-only)
            agent_qs = AgentsInformation.objects.exclude(
                agentsInformation_entity_num__isnull=True
            ).exclude(
                agentsInformation_entity_num__exact=''
            )

            # Create a defaultdict for agent entity numbers mapped to agent objects
            agents_map = defaultdict(list)
            for agent in agent_qs:
                try:
                    # Normalize to integer
                    float_entity_num = float(agent.agentsInformation_entity_num)
                    int_entity_num = int(float_entity_num)
                    agents_map[int_entity_num].append(agent)
                except ValueError:
                    continue

            # Step 2: Collect unique filing entity numbers (integer-only)
            filings_qs = FilingsInformation.objects.exclude(
                filingsInformation_entity_num__isnull=True
            ).exclude(
                filingsInformation_entity_num__exact=''
            )
            
            # Use a dictionary to ensure unique filings by entity number
            unique_filings_dict = {}
            for filing in filings_qs:
                try:
                    normalized_num = int(float(filing.filingsInformation_entity_num))
                    if normalized_num not in unique_filings_dict:
                        unique_filings_dict[normalized_num] = filing
                except ValueError:
                    continue
            unique_filings = list(unique_filings_dict.values())
            # Step 3: Match unique filings against agent map
            matched_count = 0
            unmatched_count = 0
            total_agents_matches = 0
            # Step 4: Insert matched agent records in bulk
            def insert_matched_agent_records(filing_obj, agent_objs):
                FilingAndAgentInfo = apps.get_model('FinalRecords', 'FilingAndAgentInfo')
                valid_fields = set(f.name for f in FilingAndAgentInfo._meta.get_fields())
                to_create = []
                new_data = [
                    EntityAgentMapping(
                        Entity_Table_ID=filing_obj,
                        Agent_ID=lic
                    )
                    for lic in agent_objs
                ]
                EntityAgentMapping.objects.bulk_create(new_data, batch_size=500)
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
                if to_create:
                    FilingAndAgentInfo.objects.bulk_create(to_create)

            # Step 5: Process the filings and match agents in batch
            for filing in tqdm(unique_filings, desc="Matching Unique Filings with Agents", unit="filing"):
                filing_num = int(float(filing.filingsInformation_entity_num))  # Ensure it's an integer
                agent_matches = agents_map.get(filing_num, [])
                if agent_matches:  # Checks if the list is non-empty
                    insert_matched_agent_records(filing, agent_matches)
                    matched_count += 1
                    total_agents_matches += len(agent_matches)
                else:
                    unmatched_count += 1

            # Summary
            print("\n---------- Unique Filing Match Summary ----------")
            print(f"🧾 Total Unique Filings: {len(unique_filings)}")
            print(f"✅ Matched Unique Filings: {matched_count}")
            print(f"❌ Unmatched Unique Filings: {unmatched_count}")
            print(f"🔢 Total Agent Matches (sum of all counts): {total_agents_matches}")

            # Send success message to the user
            message = '✅ Unique Filings matched successfully with Agents!'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/filingandagentinfo/")

        return FilingAndAgentInfomerge_view


@admin.register(EntityABCLicenseMapping) #Mapping Report in the Multiple CSV Genrate 
class EntityABCLicenseMappingAdmin(CustomMergeAdminMixin, admin.ModelAdmin):
    merge_url_name = "entityabclicensemapping"
    list_display = ("id", "Entity_Table_ID_id", "ABC_Licennse_ID_id", "created_at", "updated_at")
    def get_merge_view(self):
        def EntityABCLicenseMappingmerge_view(request):
            df = pd.read_csv('finalrecords_entityabclicensemapping_Unique.csv')
            output_filling_data = []
            output_abc_data_data = []
            License_agent = []
            output_agent_data = []
            License_Pricipal = []
            output_pricipal_data= []
            licensee_qs = DataSet1Record.objects.exclude(abc_licensee__isnull=True).exclude(abc_licensee__exact='')
            normalized_licensee_map = defaultdict(list)
            for lic in licensee_qs:
                normalized = normalize_name(lic.abc_licensee)
                normalized_licensee_map[normalized].append(lic)

            # Build normalized principal name map with full objects
            principal_qs = PrincipalsInformation.objects.all()
            normalized_principal_map = defaultdict(list)
            for lic in principal_qs:
                normalized = float(lic.principalsInformation_entity_num)
                normalized_principal_map[normalized].append(lic)
            agent_qs = AgentsInformation.objects.exclude(
                agentsInformation_entity_num__isnull=True
            ).exclude(
                agentsInformation_entity_num__exact=''
            )

            # Create a defaultdict for agent entity numbers mapped to agent objects
            agents_map = defaultdict(list)
            for agent in agent_qs:
                try:
                    # Normalize to integer
                    float_entity_num = float(agent.agentsInformation_entity_num)
                    int_entity_num = int(float_entity_num)
                    agents_map[int_entity_num].append(agent)
                except ValueError:
                    continue
            License_mappiung = []
            output_folder = 'Mike Final Data'
            os.makedirs(output_folder, exist_ok=True)
            for index, row in df.iterrows():
                Entity_Table_ID_id = row['Entity_Table_ID_id'] 
                print(f"Processing row {index + 1}: Entity_Table_ID_id={Entity_Table_ID_id}")
                filings_qs = FilingsInformation.objects.get(id=Entity_Table_ID_id)
                filing_name = filings_qs.filingsInformation_entity_name
                normalized = normalize_name(filing_name)
                try:
                    filing_float = float(filings_qs.filingsInformation_entity_num)
                except ValueError:
                    continue
                filings_qs_data = {field.name: getattr(filings_qs, field.name) for field in filings_qs._meta.fields}
                output_filling_data.append(filings_qs_data)
                # Abc Licensee Mapping
                licensee_matches = normalized_licensee_map.get(normalized, [])
                if licensee_matches:
                    for licensee in licensee_matches:
                        new_data = {
                            'Entity_Table_ID(Filling ID)': filings_qs.id,
                            'ABC_Licennse_ID': licensee.id
                        }
                        License_mappiung.append(new_data)
                        data_record_data = {field.name: getattr(licensee, field.name) for field in licensee._meta.fields}
                        output_abc_data_data.append(data_record_data)
                # Pricipal Mapping
                principal_matches = normalized_principal_map.get(filing_float, [])
                if principal_matches:
                    for principal in principal_matches:
                        new_data = {
                            'Entity_Table_ID(Filling ID)': filings_qs.id,
                            'Principal_ID_id': principal.id
                        }
                        License_Pricipal.append(new_data)
                        data_principal_REC = {field.name: getattr(principal, field.name) for field in principal._meta.fields}
                        output_pricipal_data.append(data_principal_REC)
                # Agent Mapping
                agent_matches = agents_map.get(filing_float, [])
                if agent_matches:
                    for agent in agent_matches:
                        new_data = {
                            'Entity_Table_ID(Filling ID)': filings_qs.id,
                            'Agent_ID': agent.id
                        }
                        License_agent.append(new_data)
                        data_Agent_data = {field.name: getattr(agent, field.name) for field in agent._meta.fields}
                        output_agent_data.append(data_Agent_data)
                # print(f"normalized: {normalized}")
                # break
            output_filling_dataMap = pd.DataFrame(output_filling_data) 
            output_filling_dataMap.to_csv(f'{output_folder}/Company_Entity_Table_Fillings.csv', index=False)

            # ABC Licensee File Genrating 
            pd_lincnec_mapping = pd.DataFrame(License_mappiung) 
            pd_lincnec_mapping.to_csv(f'{output_folder}/Entity_ABC_License_Mapping_Table.csv', index=False)
            output_abc_data_dataMap = pd.DataFrame(output_abc_data_data) 
            output_abc_data_dataMap.to_csv(f'{output_folder}/ABC_Lincense_Table.csv', index=False)

            # Principal Mapping File Genrating
            pd_principal_mapping = pd.DataFrame(License_Pricipal)
            pd_principal_mapping.to_csv(f'{output_folder}/Entity_Principal_Mapping_Table.csv', index=False)
            output_pricipal_dataMap = pd.DataFrame(output_pricipal_data)
            output_pricipal_dataMap.to_csv(f'{output_folder}/Principals_Table(Unique-).csv', index=False)
            # output_df = pd.DataFrame(output_abc_data_data)
            pd_License_agent_mapping = pd.DataFrame(License_agent)   
            pd_License_agent_mapping.to_csv(f'{output_folder}Entity_Agent_Mapping_Table.csv', index=False) 
            output_output_agent_data_dataMap = pd.DataFrame(output_agent_data)
            output_output_agent_data_dataMap.to_csv(f'{output_folder}/Agent_Table.csv', index=False)
            # output_df.to_csv('output_abc_data_data.csv', index=False)
            message = 'Data Merged successfully for Filing, Principal & Agent and saved in BusinessLocationLicense.'
            self.message_user(request, message, messages.SUCCESS)
            return HttpResponseRedirect("/admin/FinalRecords/entityabclicensemapping/")
        return EntityABCLicenseMappingmerge_view