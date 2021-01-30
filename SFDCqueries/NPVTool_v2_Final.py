import unittest
import shutil
import os
import json
import pprint
pp = pprint.PrettyPrinter(indent=2)

from datetime import date, datetime, timedelta
today = date.today()
import datetime as dt

import csv
import xlrd
from openpyxl.workbook import Workbook

from simple_salesforce import Salesforce, SalesforceLogin
from pandas import DataFrame
import pandas as pd
import numpy as np





##SOQL example to pull JSON for Quotes:
# def soqlJSON(quote):  # pylint: disable=unused-variable
#     soql_query = 'SELECT Name, CPQ_JSON__c \
#     FROM CPQ__c \
#     WHERE (Name = "Quote-2347669")'


### DATA QUERY PARAMETERS:
## SOQL

class EXTRACT:
    @staticmethod
    def get_opp_info():
        ### Expand or reduce opp types and/or RBG's as needed. Adjust limit as needed (total data size)
        soql = "SELECT Id, Name, CloseDate, Opportunity_Type__c, Total_MRR_Calc__c, Bandwidth__c, Reporting_Business_Group__c, Opportunity_Workflow_Type__c, StageName, Tax_Fee_Pass_Through__c, Installation_NRR__c, Total_MAR__c, Total_MRR__c, Term_in_Months__c, NPV__c, Netex_MRC_Approved__c, Netex_NRC_Approved__c, Total_Success_Based_Capex_Calc__c, Amount, Account.Name from Opportunity where Reporting_Business_Group__c in ('Ethernet', 'CloudLink', 'WAN') and StageName = '4 - Closed' and NPV__c >= 0 and CloseDate >= 2021-01-01 and Opportunity_Type__c in ('New Service', 'Positive Re-Rate/Move/Change', 'Negative Re-Rate/Move/Change') and Account.Intercompany_Account__c = false LIMIT 100"
        return soql

    def get_npv_task(self, stringers):
        soql = "SELECT Id, Subject, WhatId, Additional_Task_Response_Notes__c, Status from Task where WhatId in (" + stringers + ") and Subject = 'NPV Validation' and Status in ('Not Started', 'In Progress')"
        return soql

    def get_so_info(self, stringers):
        soql = "SELECT Id, Order_Stage__c, ICB_Lookup__c, Cor_Form__c, Name, Opportunity__c, CPQ__c, Tax_Fee_Pass_Through__c, NRR__c, MAR__c, MRR__c, Total_MRR__c, Term__c, Bandwidth__c, Allocated_NPV_Calc__c, Netx_MRC__c, Netx_NRC__c, SO_Renewal_Interval_Months__c, SO_Notice_Period_Days__c from Service_Order__c  WHERE Opportunity__c in (" + stringers + ") and Order_Stage__c in ('6 - Pipeline', '7 - Ready for Billing', '8 - Accepted by Billing', '9 - Billing Updated')"
        return soql

    def get_capital_project_info(self, stringers):
        soql = "SELECT Id, Status__c, Name, Opportunity__c, ICB_Approved_Amount__c from Capital_Project__c  Where Status__c != 'Cancelled' and Opportunity__c in (" + stringers + ")"
        return soql

    def get_expense_builder_info(self, stringers):
        soql = "SELECT Id, Status__c, Name, Opportunity__c, Netex_MRC__c, Netex_NRC__c from NetEx_Builder__c Where Status__c != 'Cancelled' and Opportunity__c in (" + stringers + ")"
        return soql

    def get_quote_info(self, stringers):
        soql = "SELECT Id, CPQ_Status__c, Name, Opportunity__c, Total_Capex__c, X12_Netex_MRC__c, X12_Netex_NRC__c, X24_Netex_MRC__c, X24_Netex_NRC__c, X36_Netex_MRC__c, X36_Netex_NRC__c, X60_Netex_MRC__c, X60_Netex_NRC__c, X12_MRC_Quote__c, X24_MRC_Quote__c, X36_MRC_Quote__c, X60_MRC_Quote__c, X12_NRR_Quote__c, X24_NRR_Quote__c, X36_NRR_Quote__c, X60_NRR_Quote__c from CPQ__c  Where Opportunity__c in (" + stringers + ")"
        return soql

    def get_cor_form_info(self, stringers):
        soql = "SELECT Id, Name, Special_Pricing_ICB__c, Special_Pricing_ICB__r.Opportunity_Lookup__c, Expense_MRC_Yr1__c, Expense_NRC_Yr1__c, Expense_MRC_Yr2__c, Expense_NRC_Yr2__c, Expense_MRC_Yr3__c, Expense_NRC_Yr3__c, Expense_MRC_Yr5__c, Expense_NRC_Yr5__c, Total_Success_Based_Capital__c from Cor_Form__c Where Special_Pricing_ICB__r.Status__c = 'Approved' and Special_Pricing_ICB__r.Opportunity_Lookup__c in (" + stringers + ")"
        return soql

    
    def format_opp_ids(self, Opps, size):
        stringers = ''
        for x in range(0,size-1):
            stringers = "'" + Opps['records'][x]['Id'] + "'," + stringers
        stringers = stringers + "'" + Opps['records'][size-1]['Id'] + "'"
        return stringers


## Execution of SOQL Queries (**modify boolean as needed, if testing directly from static JSON blobs)
fullSoqlQueryMode = True

## SFDC API AUTH:
# auth is a separate Python module as a placeholder to store SFDC creds. Ref required params as follows:
# sf = Salesforce(username='username', password='password', security_token='token', client_id='Testing', \
    # instance_url='https://zayo.my.salesforce.com', session_id='')

if fullSoqlQueryMode == True:
    from auth import sf
    temp = EXTRACT()
    OppQuery = temp.get_opp_info()
    OppOutput = sf.query_all(OppQuery)
    # recordsonly = OppOutput['records']
    size = OppOutput['totalSize']
    # pp.pprint(OppOutput)

    stringers = temp.format_opp_ids(OppOutput, size)
    NPVTaskQuery = temp.get_npv_task(stringers)
    NPVTaskOutput = sf.query_all(NPVTaskQuery)
    # pp.pprint(NPVTaskOutput)

    ServiceOrderQuery = temp.get_so_info(stringers)
    ServiceOrderOutput = sf.query_all(ServiceOrderQuery)
    # pp.pprint(ServiceOrderOutput)

    CapProjQuery = temp.get_capital_project_info(stringers)
    CapProjOutput = sf.query_all(CapProjQuery)
    # pp.pprint(CapProjOutput)

    ExpenseBuilderQuery = temp.get_expense_builder_info(stringers)
    ExpenseBuilderOutput = sf.query_all(ExpenseBuilderQuery)
    # pp.pprint(ExpenseBuilderOutput)

    QuoteQuery = temp.get_quote_info(stringers)
    QuoteOutput = sf.query_all(QuoteQuery)
    # pp.pprint(QuoteOutput)

    CORFormQuery = temp.get_cor_form_info(stringers)
    CORFormOutput = sf.query_all(CORFormQuery)
    # pp.pprint(CORFormOutput)

    finalDictList = [
        OppOutput,
        NPVTaskOutput,
        ServiceOrderOutput,
        CapProjOutput,
        ExpenseBuilderOutput,
        QuoteOutput,
        CORFormOutput
    ]

    # pp.pprint(finalDictList)
    # for d in finalDictList:
    #     print(len(d['records']))


### JSON Dump for Testing (**modify this block as needed, if running in production/directly from the SimpleSalesforce API)
## Edit dir structure as appropriate:
localPathList = 'C:/Users/tvaroglu/Desktop/Product Dev/Coding/Testing/SFDCTools/SFDCqueries/'
blobMaster = localPathList + 'SOQL_JSON_Shell.json'

## Modify booleans as needed (read/write vs read only):
jDumpWrite = False
jDumpRead = False

if jDumpWrite == True and fullSoqlQueryMode == True:
    with open(blobMaster, 'w') as writer:
        json.dump(finalDictList, writer, indent=4)

if jDumpRead == True:
    with open(blobMaster, 'r') as reader:
        envdata = json.load(reader)
        # pp.pprint(envdata)

print('All data successfully queried. Any errors after this point are due to DATA VALIDATION ONLY.')



### TRANSFORM
if fullSoqlQueryMode == True:
    targetDictList = finalDictList
else:
    targetDictList = envdata

oppsToValidate = targetDictList[0]['records']
SOsToValidate = targetDictList[2]['records']
quotesToValidate = targetDictList[5]['records']
targetCORforms = targetDictList[6]['records']
CPsToValidate = targetDictList[3]['records']
EBsToValidate = targetDictList[4]['records']
npvTasksToValidate = targetDictList[1]['records']


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
# print(finalOppToTaskIdOutput)
## Everything is correct, NPV task can now be validated:
npvTasksToClose = []
for f in finalOppToTaskIdOutput:
    for n in npvTasksToValidate:
        taskId = n['Id']
        associatedOppId = n['WhatId']
        if f == associatedOppId and taskId not in npvTasksToClose:
            npvTasksToClose.append(taskId)
for task in npvTasksToClose:
    test = sf.Task.update(
        task, 
        {
            'Status': 'Completed', 
            'OwnerId': '00560000001hYAUAA2', 
            'Additional_Task_Response_Notes__c': 
            'Completed via Automation'
            }
            )
if len(npvTasksToClose) == 0:
    print('0 NPV tasks validated by automation.')
elif len(npvTasksToClose) > 0:
    print('{} NPV tasks {} validated by automation. Please move to Stage 5 via the corresponding report queue: \n \
        https://zayo.my.salesforce.com/00O0z000005btK4'.format(len(npvTasksToClose), npvTasksToClose))
