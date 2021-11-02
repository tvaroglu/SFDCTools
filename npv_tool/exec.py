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
print('All data successfully queried. Any errors after this point are due to DATA VALIDATION ONLY.')



oppsToValidate = target_dict_list[0]['records']
SOsToValidate = target_dict_list[2]['records']
quotesToValidate = target_dict_list[5]['records']
targetCORforms = target_dict_list[6]['records']
CPsToValidate = target_dict_list[3]['records']
EBsToValidate = target_dict_list[4]['records']
npvTasksToValidate = target_dict_list[1]['records']


## This block checks Opp vs SO financial field values. If anything doesn't align between the two, the Opp is excluded from future validation stages.
cleanOppToSOs = []
for o in oppsToValidate:
    opptyID = o['Id']
    opptyAmount = o['Amount']
    opptyTax = o['Tax_Fee_Pass_Through__c']
    opptyNRR = o['Installation_NRR__c']
    opptyMAR = o['Total_MAR__c']
    opptyTerm = o['Term_in_Months__c']
    opptyNetexMRC = o['Netex_MRC_Approved__c']
    opptyNetexNRC = o['Netex_NRC_Approved__c']
    for s in SOsToValidate:
        soRawMRR = s['MRR__c'] - s['Tax_Fee_Pass_Through__c']
        soTax = s['Tax_Fee_Pass_Through__c']
        soNRR = s['NRR__c']
        soMAR = s['MAR__c']
        soTerm = s['Term__c']
        soNetexMRC = s['Netx_MRC__c']
        soNetexNRC = s['Netx_NRC__c']
        if opptyID == s['Opportunity__c']:
            amountCheck = opptyAmount == soRawMRR
            taxCheck = opptyTax == soTax
            nrrCheck = opptyNRR == soNRR
            marCheck = opptyMAR == soMAR
            termCheck = opptyTerm == soTerm
            netexMRCheck = opptyNetexMRC == soNetexMRC
            netexNRCheck = opptyNetexNRC == soNetexNRC
            if amountCheck and taxCheck and nrrCheck and marCheck and termCheck and netexMRCheck and netexNRCheck and opptyID not in cleanOppToSOs:
                # print(opptyID, s['Opportunity__c'])
                # print(opptyAmount, soRawMRR)
                # print(opptyTax, soTax)
                # print(opptyMAR, soMAR)
                # print(opptyTerm, soTerm)
                # print(opptyNetexMRC, soNetexMRC)
                # print(opptyNetexMRC, soNetexMRC)
                cleanOppToSOs.append(opptyID)
# print(cleanOppToSOs)

## This block checks Opp vs Quote or COR form, based on Workflow Type. If anything doesn't align between the two, the Opp is excluded from future validation stages.
cleanOppToCORorQuote = []
validTermLengths = [12.0, 24.0, 36.0, 60.0]
for c in cleanOppToSOs:
    for o in oppsToValidate:
        opptyID = o['Id']
        opptyCapex = o['Total_Success_Based_Capex_Calc__c']
        opptyTerm = o['Term_in_Months__c']
        opptyMRR = o['Amount']
        opptyNRR = o['Installation_NRR__c']
        opptyNetexMRC = o['Netex_MRC_Approved__c']
        opptyNetexNRC = o['Netex_NRC_Approved__c']
        opptyWorkflowType = o['Opportunity_Workflow_Type__c']
        if c == opptyID and opptyWorkflowType in ('Standard Pricing', 'Tranzact Custom Solution', 'ASR (Automated)'):
            for q in quotesToValidate:
                relatedOppty = q['Opportunity__c']
                totalCapex = q['Total_Capex__c']
                quote12MRC = q['X12_MRC_Quote__c']
                quote12NRC = q['X12_NRR_Quote__c']
                quote24MRC = q['X24_MRC_Quote__c']
                quote24NRC = q['X24_NRR_Quote__c']
                quote36MRC = q['X36_MRC_Quote__c']
                quote36NRC = q['X36_NRR_Quote__c']
                quote60MRC = q['X60_MRC_Quote__c']
                quote60NRC = q['X60_NRR_Quote__c']
                quoteNetx12MRC = q['X12_Netex_MRC__c']
                quoteNetx12NRC = q['X12_Netex_NRC__c']
                quoteNetx24MRC = q['X24_Netex_MRC__c']
                quoteNetx24NRC = q['X24_Netex_NRC__c']
                quoteNetx36MRC = q['X36_Netex_MRC__c']
                quoteNetx36NRC = q['X36_Netex_NRC__c']
                quoteNetx60MRC = q['X60_Netex_MRC__c']
                quoteNetx60NRC = q['X60_Netex_NRC__c']
                if opptyID == relatedOppty and q['CPQ_Status__c'] in ('Ordered', 'Order Pending'):
                    capexCheck = opptyCapex == totalCapex
                    if opptyTerm == 12.0:
                        custMRCheck = opptyMRR == quote12MRC
                        custNRCheck = opptyNRR == quote12NRC
                        netxMRCheck = opptyNetexMRC == quoteNetx12MRC
                        netxNRCheck = opptyNetexNRC == quoteNetx12NRC
                    elif opptyTerm == 24.0:
                        custMRCheck = opptyMRR == quote24MRC
                        custNRCheck = opptyNRR == quote24NRC
                        netxMRCheck = opptyNetexMRC == quoteNetx24MRC
                        netxNRCheck = opptyNetexNRC == quoteNetx24NRC
                    elif opptyTerm == 36.0:
                        custMRCheck = opptyMRR == quote36MRC
                        custNRCheck = opptyNRR == quote36NRC
                        netxMRCheck = opptyNetexMRC == quoteNetx36MRC
                        netxNRCheck = opptyNetexNRC == quoteNetx36NRC
                    elif opptyTerm == 60.0:
                        custMRCheck = opptyMRR == quote60MRC
                        custNRCheck = opptyNRR == quote60NRC
                        netxMRCheck = opptyNetexMRC == quoteNetx60MRC
                        netxNRCheck = opptyNetexNRC == quoteNetx60NRC
                    elif opptyTerm not in validTermLengths:
                        continue
                    if capexCheck and custMRCheck and custNRCheck and netxMRCheck and netxNRCheck and c not in cleanOppToCORorQuote:
                        # print(c, opptyID, relatedOppty)
                        # print(opptyCapex, totalCapex)
                        # print(opptyTerm)
                        # print(opptyMRR, opptyNRR)
                        # print(quote12MRC, quote12NRC)
                        # print(quote24MRC, quote24NRC)
                        # print(quote36MRC, quote36NRC)
                        # print(quote60MRC, quote60NRC)
                        # print(opptyNetexMRC, opptyNetexNRC)
                        # print(quoteNetx12MRC, quoteNetx12NRC)
                        # print(quoteNetx24MRC, quoteNetx24NRC)
                        # print(quoteNetx36MRC, quoteNetx36NRC)
                        # print(quoteNetx60MRC, quoteNetx60NRC)
                        cleanOppToCORorQuote.append(c)
        elif c == opptyID and opptyWorkflowType == 'Custom Solution':
            for t in targetCORforms:
                relatedOppty = t['Special_Pricing_ICB__r']['Opportunity_Lookup__c']
                totalCapex = t['Total_Success_Based_Capital__c']
                NetexMRC12mo = t['Expense_MRC_Yr1__c']
                NetexNRC12mo = t['Expense_NRC_Yr1__c']
                NetexMRC24mo = t['Expense_MRC_Yr2__c']
                NetexNRC24mo = t['Expense_NRC_Yr2__c']
                NetexMRC36mo = t['Expense_MRC_Yr3__c']
                NetexNRC36mo = t['Expense_NRC_Yr3__c']
                NetexMRC60mo = t['Expense_MRC_Yr5__c']
                NetexNRC60mo = t['Expense_NRC_Yr5__c']
                if opptyID == relatedOppty:
                    capexCheck = opptyCapex == totalCapex
                    if opptyTerm == 12.0:
                        netxMRCheck = opptyNetexMRC == NetexMRC12mo
                        netxNRCheck = opptyNetexNRC == NetexNRC12mo
                    elif opptyTerm == 24.0:
                        netxMRCheck = opptyNetexMRC == NetexMRC24mo
                        netxNRCheck = opptyNetexNRC == NetexNRC24mo
                    elif opptyTerm == 36.0:
                        netxMRCheck = opptyNetexMRC == NetexMRC36mo
                        netxNRCheck = opptyNetexNRC == NetexNRC36mo
                    elif opptyTerm == 60.0:
                        netxMRCheck = opptyNetexMRC == NetexMRC60mo
                        netxNRCheck = opptyNetexNRC == NetexNRC60mo
                    elif opptyTerm not in validTermLengths:
                        continue
                    if capexCheck and netxMRCheck and netxNRCheck and c not in cleanOppToCORorQuote:
                        # print(c, opptyID, relatedOppty)
                        # print(opptyCapex, totalCapex)
                        # print(opptyTerm)
                        # print(opptyNetexMRC, opptyNetexNRC)
                        # print(NetexMRC12mo, NetexNRC12mo)
                        # print(NetexMRC24mo, NetexNRC24mo)
                        # print(NetexMRC36mo, NetexNRC36mo)
                        # print(NetexMRC60mo, NetexNRC60mo)
                        cleanOppToCORorQuote.append(c)
        ## If neither Tranzact nor ICB workflow, Opp is assumed to be clean based on initial Opp vs SO check, and is added to future validation stage.
        ## Can REMOVE these final lines, if false positives are produced as a result:
        elif c == opptyID and opptyWorkflowType == None and c not in cleanOppToCORorQuote:
            cleanOppToCORorQuote.append(c)
        elif c == opptyID and opptyWorkflowType == 'Administrative' and c not in cleanOppToCORorQuote:
            cleanOppToCORorQuote.append(c)
# print(cleanOppToCORorQuote)

## This final block checks Opp vs EB & CP values, as applicable. If this stage is passed, the associated NPV task can be closed.
cleanOpptoCPandEB = []
cpOppShell = {}
ebOppShell = {}
for p in CPsToValidate:
    cpOppShell[p['Opportunity__c']] = p['ICB_Approved_Amount__c']
for clean in cleanOppToCORorQuote:
    totalNetxMRC = 0
    totalNetxNRC = 0
    for e in EBsToValidate:
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
    for o in oppsToValidate:
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
#     for n in npvTasksToValidate:
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
