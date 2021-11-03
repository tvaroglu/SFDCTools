import pprint
pp = pprint.PrettyPrinter(indent=2)
import shutil
import os
import json
import csv
from datetime import date, datetime, timedelta
import datetime as dt
today = date.today()

from extract import Extractor
from transform import Transformer
## Modify boolean as needed, if testing directly from static JSON blobs or performing execution of SOQL queries:
full_soql_query_mode = False

## SFDC API AUTH:
# auth is a separate Python module as a placeholder to store SFDC creds. Ref required params as follows:
# sf = Salesforce(username='username', password='password', security_token='token', client_id='Testing', \
    # instance_url='https://zayo.my.salesforce.com', session_id='')
if full_soql_query_mode == True:
    from simple_salesforce import Salesforce, SalesforceLogin
    from auth import sf

    e = Extractor()
    opp_query = e.get_opp_info()
    opp_output = sf.query_all(opp_query)
    records_only = opp_output['records']
    size = opp_output['totalSize']
    formatted_opp_ids = e.format_opp_ids(opp_output, size)

    npv_task_query = e.get_npv_task(formatted_opp_ids)
    npv_task_output = sf.query_all(npv_task_query)

    service_order_query = e.get_so_info(formatted_opp_ids)
    service_order_output = sf.query_all(service_order_query)

    cap_proj_query = e.get_capital_project_info(formatted_opp_ids)
    cap_proj_output = sf.query_all(cap_proj_query)

    expense_builder_query = e.get_expense_builder_info(formatted_opp_ids)
    expense_builder_output = sf.query_all(expense_builder_query)

    quote_query = e.get_quote_info(formatted_opp_ids)
    quote_output = sf.query_all(quote_query)

    cor_form_query = e.get_cor_form_info(formatted_opp_ids)
    cor_form_output = sf.query_all(cor_form_query)

    final_dict_list = [
        opp_output,
        npv_task_output,
        service_order_output,
        cap_proj_output,
        expense_builder_output,
        quote_output,
        cor_form_output
    ]

base_path = os.path.dirname(__file__)
blob_master = f'{base_path}/SOQL_shell.json'
### Modify booleans as needed
## (read/write vs read only, if running in production/directly from the SimpleSalesforce API):
j_dump_write = False
j_dump_read = True

if j_dump_write == True and full_soql_query_mode == True:
    with open(blob_master, 'w') as writer:
        json.dump(final_dict_list, writer, indent=4)

if j_dump_read == True:
    with open(blob_master, 'r') as reader:
        envdata = json.load(reader)

if full_soql_query_mode == True:
    target_dict_list = final_dict_list
else:
    target_dict_list = envdata

opps = target_dict_list[0]['records']
service_orders = target_dict_list[2]['records']
quotes = target_dict_list[5]['records']
cor_forms = target_dict_list[6]['records']
cap_projects = target_dict_list[3]['records']
expense_builders = target_dict_list[4]['records']
npv_tasks = target_dict_list[1]['records']

print('All data successfully queried. Any errors after this point are due to DATA VALIDATION ONLY.')


t = Transformer(opps, service_orders, quotes, cor_forms, cap_projects, expense_builders)
valid_opp_to_service_orders = t.validate_opp_to_service_order()

cleanOppToCORorQuote = t.validate_opp_to_quote_or_cor_form(valid_opp_to_service_orders)

## This final block checks Opp vs EB & CP values, as applicable. If this stage is passed, the associated NPV task can be closed.
cleanOpptoCPandEB = []
cpOppShell = {}
ebOppShell = {}
for p in cap_projects:
    cpOppShell[p['Opportunity__c']] = p['ICB_Approved_Amount__c']
for clean in cleanOppToCORorQuote:
    totalNetxMRC = 0
    totalNetxNRC = 0
    for e in expense_builders:
        Id = e['Id']
        oppID = e['Opportunity__c']
        netXMRC = e['Netex_MRC__c']
        netXNRC = e['Netex_NRC__c']
        if clean == oppID:
            totalNetxMRC += netXMRC
            totalNetxNRC += netXNRC
            ebOppShell[clean] = [
                totalNetxMRC, totalNetxNRC
                ]
# pp.pprint(cpOppShell)
# pp.pprint(ebOppShell)
for cleaned in cleanOppToCORorQuote:
    for o in opps:
        opptyID = o['Id']
        opptyCapex = o['Total_Success_Based_Capex_Calc__c']
        opptyNetexMRC = o['Netex_MRC_Approved__c']
        opptyNetexNRC = o['Netex_NRC_Approved__c']
        if cleaned == opptyID:
            for p in cpOppShell:
                relatedOpp = p
                capex = cpOppShell[p]
                if opptyID == relatedOpp:
                    capCk = opptyCapex == capex
                    if capCk:
                        # print(cleaned, opptyID, relatedOpp)
                        # print(opptyCapex, capex)
                        cpOppShell[p] = 'Clean'
                    else:
                        cpOppShell[p] = 'Dirty'
            for b in ebOppShell:
                relatedOpp = b
                totalNetexMRC = ebOppShell[b][0]
                totalNetexNRC = ebOppShell[b][1]
                if opptyID == relatedOpp:
                    MRCheck = opptyNetexMRC == totalNetexMRC
                    NRCheck = opptyNetexNRC == totalNetexNRC
                    if MRCheck and NRCheck:
                        # print(cleaned, opptyID, relatedOpp)
                        # print(opptyNetexMRC, totalNetexMRC)
                        # print(opptyNetexNRC, totalNetexNRC)
                        ebOppShell[b] = 'Clean'
                    else:
                        ebOppShell[b] = 'Dirty'
# pp.pprint(cpOppShell)
# pp.pprint(ebOppShell)

## This final block preps the final clean Opp Id list, so the associated NPV task can be closed.
finalOppToTaskIdOutput = []
for c in cleanOppToCORorQuote:
    ## If there is no CP or EB, assumed clean, because already validated in prior two stages.
    # Remove this block if false positives are produced:
    if c not in cpOppShell.keys() and c not in ebOppShell.keys() and c not in finalOppToTaskIdOutput:
        finalOppToTaskIdOutput.append(c)
    ## If prior two validation stages passed, and NOT 'Dirty' from CP check:
    elif c in cpOppShell.keys() and c not in ebOppShell.keys():
        for p in cpOppShell:
            if c == p and cpOppShell[p] == 'Clean' and c not in finalOppToTaskIdOutput:
                finalOppToTaskIdOutput.append(c)
    ## If prior two validation stages passed, and NOT 'Dirty' from EB check:
    elif c not in cpOppShell.keys() and c in ebOppShell.keys():
        for e in ebOppShell:
            if c == e and ebOppShell[e] == 'Clean' and c not in finalOppToTaskIdOutput:
                finalOppToTaskIdOutput.append(c)
    ## If prior two validation stages passed, and NOT 'Dirty' from both CP AND EB check:
    elif c in cpOppShell.keys() and c in ebOppShell.keys():
        for p in cpOppShell:
            cpCk = False
            if c == p and cpOppShell[p] == 'Clean':
                cpCk = True
            for e in ebOppShell:
                ebCk = False
                if c == e and ebOppShell[e] == 'Clean':
                    ebCk = True
                if cpCk and ebCk and c not in finalOppToTaskIdOutput:
                    finalOppToTaskIdOutput.append(c)
print(finalOppToTaskIdOutput)
## Everything is correct, NPV task can now be validated:
# npvTasksToClose = []
# for f in finalOppToTaskIdOutput:
#     for n in npv_tasks:
#         taskId = n['Id']
#         associatedOppId = n['WhatId']
#         if f == associatedOppId and taskId not in npvTasksToClose:
#             npvTasksToClose.append(taskId)
# for task in npvTasksToClose:
#     test = sf.Task.update(
#         task,
#         {
#             'Status': 'Completed',
#             'OwnerId': '00560000001hYAUAA2',
#             'Additional_Task_Response_Notes__c':
#             'Completed via Automation'
#             }
#             )
# if len(npvTasksToClose) == 0:
#     print('0 NPV tasks validated by automation.')
# elif len(npvTasksToClose) > 0:
#     print('{} NPV tasks {} validated by automation. Please move to Stage 5 via the corresponding report queue: \n \
#         https://zayo.my.salesforce.com/00O0z000005btK4'.format(len(npvTasksToClose), npvTasksToClose))
