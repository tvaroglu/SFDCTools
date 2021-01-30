import unittest
import shutil
import os
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



### SFDC API AUTH:
from auth import sf
## auth is a separate Python module as a placeholder to store SFDC creds. 
    # ref required params:
## sf = Salesforce(username=username, password=password, security_token=token, client_id='Testing', \
    #instance_url="https://zayo.my.salesforce.com", session_id='')



### DATA QUERY PARAMETERS:
## SOQL

opp_data = sf.query_all("SELECT Id, Opportunity_Id__c, Reporting_Business_Group__c, Opportunity_Workflow_Type__c, StageName, Tax_Fee_Pass_Through__c, Installation_NRR__c, Total_MAR__c, Total_MRR__c, Term_in_Months__c, NPV__c, Netex_MRC_Approved__c, Netex_NRC_Approved__c, Total_Success_Based_Capex_Calc__c from Opportunity  Where Reporting_Business_Group__c = 'Ethernet' and Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and StageName = '4 - Closed' and NPV__c >0 LIMIT 10")
so_data = sf.query_all("SELECT Id, Order_Stage__c, Name, Opportunity__r.Reporting_Business_Group__c, Opportunity__r.Opportunity_Workflow_Type__c, Opportunity__r.StageName, Opportunity__r.NPV__c, Opportunity__c, CPQ__c, Tax_Fee_Pass_Through__c, NRR__c, MAR__c, Total_MRR__c, Term__c, Allocated_NPV_Calc__c, Netx_MRC__c, Netx_NRC__c from Service_Order__c  Where Order_Stage__c = '6 - Pipeline' and Opportunity__r.Reporting_Business_Group__c = 'Ethernet' and Opportunity__r.Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and Opportunity__r.StageName = '4 - Closed' and Opportunity__r.NPV__c >0 LIMIT 10")
quote_data = sf.query_all("SELECT Id, CPQ_Status__c, Name, Opportunity__c, Opportunity__r.Reporting_Business_Group__c, Opportunity__r.Opportunity_Workflow_Type__c, Opportunity__r.StageName, Opportunity__r.NPV__c, Total_Success_Based_Capex__c, X12_Netex_MRC__c, X12_Netex_NRC__c, X24_Netex_MRC__c, X24_Netex_NRC__c, X36_Netex_MRC__c, X36_Netex_NRC__c, X60_Netex_MRC__c, X60_Netex_NRC__c from CPQ__c  Where CPQ_Status__c = 'Ordered' and Opportunity__r.Reporting_Business_Group__c = 'Ethernet' and Opportunity__r.Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and Opportunity__r.StageName = '4 - Closed' and Opportunity__r.NPV__c >0 LIMIT 10")
quote_loc_data = sf.query_all("SELECT Id, Location__c, CPQ__c, CPQ__r.CPQ_Status__c, CPQ__r.Reporting_Business_Group__c from CPQ_Location__c Where CPQ__r.CPQ_Status__c = 'Ordered' and CPQ__r.Reporting_Business_Group__c = 'Ethernet' LIMIT 10")
soc_data = sf.query_all("SELECT Id, Status__c, Location_A__c, Service_Order__c, Service_Order__r.Reporting_Business_Group__c, Service_Order__r.Opportunity_Workflow_Type__c, Service_Order__r.Order_Stage__c, Service_Order__r.Allocated_NPV_Calc__c from Service_Order_Component__c  Where Status__c != 'Cancelled' and Service_Order__r.Reporting_Business_Group__c = 'Ethernet' and Service_Order__r.Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and Service_Order__r.Order_Stage__c = '6 - Pipeline' and Service_Order__r.Allocated_NPV_Calc__c >0 LIMIT 10")
eb_data = sf.query_all("SELECT Id, Status__c, Name, Service_Order__c, Service_Order__r.Order_Stage__c, Service_Order__r.Reporting_Business_Group__c, Service_Order__r.Opportunity_Workflow_Type__c, Opportunity__r.StageName, Opportunity__r.NPV__c, Opportunity__c, Netex_MRC__c, Netex_NRC__c from NetEx_Builder__c Where Status__c != 'Cancelled' and Service_Order__r.Reporting_Business_Group__c = 'Ethernet' and Service_Order__r.Order_Stage__c = '6 - Pipeline' and Service_Order__r.Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and Opportunity__r.StageName = '4 - Closed' and Opportunity__r.NPV__c >0 LIMIT 10")
cp_data = sf.query_all("SELECT Id, Status__c, Name, Opportunity__c, Opportunity__r.Reporting_Business_Group__c, Opportunity__r.Opportunity_Workflow_Type__c, Opportunity__r.StageName, Opportunity__r.NPV__c, ICB_Approved_Amount__c from Capital_Project__c  Where Status__c != 'Cancelled' and Opportunity__r.Reporting_Business_Group__c = 'Ethernet' and Opportunity__r.Opportunity_Workflow_Type__c IN ('Standard Pricing', 'Standard Near Net', 'Tranzact Custom Solution', 'ASR (Automated)') and Opportunity__r.StageName = '4 - Closed' and Opportunity__r.NPV__c >0 LIMIT 10")

## Pandas DataFrame Outputs

opp_df = pd.DataFrame(opp_data['records']).drop(columns='attributes', axis = 1)
so_df = pd.DataFrame(so_data['records']).drop(columns='attributes', axis = 1)
quote_df = pd.DataFrame(quote_data['records']).drop(columns='attributes', axis = 1)
quote_loc_df = pd.DataFrame(quote_loc_data['records']).drop(columns='attributes', axis = 1)
soc_df = pd.DataFrame(soc_data['records']).drop(columns='attributes', axis = 1)
eb_df = pd.DataFrame(eb_data['records']).drop(columns='attributes', axis = 1)
cp_df = pd.DataFrame(cp_data['records']).drop(columns='attributes', axis = 1)


df = [opp_df, so_df, quote_df, quote_loc_df, soc_df, eb_df, cp_df]
data = [opp_data, so_data, quote_data, quote_loc_data, soc_data, eb_data, cp_data]

for d in df:
    #print('Record Count {0}'.format(<object>_data['totalSize']))
    print(d)
    
for d in data:
    pp.pprint(d)



cor_form = sf.query_all("SELECT Id, Name, Special_Pricing_ICB__c, Special_Pricing_ICB__r.Opportunity_Lookup__c, Expense_MRC_Yr1__c, Expense_NRC_Yr1__c, Expense_MRC_Yr2__c, Expense_NRC_Yr2__c, Expense_MRC_Yr3__c, Expense_NRC_Yr3__c, Expense_MRC_Yr5__c, Expense_NRC_Yr5__c, Total_Success_Based_Capital__c from Cor_Form__c Where Special_Pricing_ICB__r.Status__c = 'Approved' and Special_Pricing_ICB__r.Approved_Date__c = LAST_N_DAYS:180 LIMIT 10")
cor_df = pd.DataFrame(cor_form['records']).drop(columns='attributes', axis = 1)
pp.pprint(cor_df)

# "SELECT Id, Opportunity__c, Location__c from Opp_Location__c WHERE Opportunity__c in (" + stringers + " ) and Name != null"
stringers = '0060z00002464O7AAI', '0066000001pqfu2AAA'
stringers = str(stringers)
opp_loc_test = sf.query_all("SELECT Id, Opportunity__c, Location__c from Opp_Location__c WHERE Opportunity__c in " + stringers + " and Name != null")
pp.pprint(opp_loc_test)





#import json


#j = {
#    "objects": "opp, ...",
#    "num records": "10",
#    "queried": "true",
#}


#pp.pprint(json.dumps(j, indent=4, sort_keys=False, separators=(".", ": ")))
