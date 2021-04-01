import unittest
import shutil
import os
import pprint
pp = pprint.PrettyPrinter(indent=2)

from datetime import date, datetime, timedelta
today = date.today()
import datetime as dt
from FileFinder import retrieveWoWFile

import csv
import xlrd, openpyxl
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


## DATA QUERY PARAMETERS:

# 1.) Change Date Range as desired. Note, specific SOQL formatting is required!
#formatting ref:  https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_select_dateformats.htm
date_range = 'THIS_YEAR'

# 2.) Change Object as desired. Note, field list and pivots will need to be revised accordingly!
sf_object = 'Opportunity'

# 3.) Change RBG(s) as desired
rbg_list = "'WAN', 'Ethernet', 'Cloudlink'"
rbgs = rbg_list.strip('"')

# 4.) Cap number of Records to shrink query (i.e. for testing), as desired
num_recs = '35000'

# 5.) Enter fields to query within field list
field_list = \
    'Id, Region_Reporting__c, Reporting_Business_Group__c, Name, Amount, Total_MRR_Calc__c, \
    Account_Owner_s_Sales_Channel__c, Owner_Id__c, Opportunity_Workflow_Type__c, Master_Agent__c, \
    Customer_Account__c, Agent_Sale__c, Total_MAR__c, Confidence_Level__c, CloseDate, Agent__c, \
    Tax_Fee_Pass_Through__c, Term_in_Months__c, Total_MRR__c, Total_NRR__c, Sub_Agent__c, \
    StageName, Account_Vertical__c, Total_Success_Based_Capex_Calc__c, OSP_Capex__c, IsMACD__c, \
    ISP_Capex__c, Transport_Capex__c, Opportunity_Type__c, IP_Capex__c, Capital_Project_ID__c, \
    Netex_MRC_Approved__c, Netex_NRC_Approved__c, On_net_Off_net__c, Netx_Required__c, \
    Approval_Status__c, ICB_Number__c, Bandwidth__c, Product_Group__c, Product__c, PAR_Count__c, \
    Product_Category__c, Num_Opp_Locations__c, Owners_Sales_Channel__c, Capital_Labor_Capex__c, \
    ForecastCategoryName, NextStep, Additional_Proposal_Notes__c'
field_query = field_list.strip("'")

soql_data = sf.query_all("SELECT {} from {} Where CloseDate = {} and \
    Account.Intercompany_Account__c != True and Customer_Account__c NOT IN \
    ('Zayo Group', 'Zayo Fiber Solutions', 'Embark Test Account', 'Test Account - Tranzact', 'General Test Account') \
    and Reporting_Business_Group__c IN({}) LIMIT {}" \
        .format(field_query, sf_object, date_range, rbgs, num_recs))
df = pd.DataFrame(soql_data['records']).drop(columns='attributes')



### FILE HANDLING:

## Directory info for .XLSX creation process:
local_path = 'C:/Users/tvaroglu/Desktop/Product Dev/Coding/Testing/DataArchive'
data_sheet_name = 'Raw Data'
pivot_index = ['Reporting_Business_Group__c', 'Region_Reporting__c', 'StageName']
destination = 'K:/Shared drives/WAN-Eth_FunnelDataArchive'

## Select current quarter or previous quarter to specify snapshot type:
desired_snapshot = 'current_quarter'
#desired_snapshot = 'previous_quarter'

current_quarter = int((today.month - 1) / 3) + 1
previous_quarter = current_quarter - 1

if desired_snapshot == 'current_quarter':
    file_name = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, today)
    pivot_sheet_name = 'CQ Anticip Sales & Churn'
    stages = ['2 - Best Case', '3 - Committed', '4 - Closed', '5 - Accepted']
    target_quarter = current_quarter
    comparison_file = retrieveWoWFile(destination)
    comparison_vs = str(comparison_file).partition('__')
    snapshot_vs = comparison_vs[-1].strip('.xlsx')
    variance_tab = 'VAR vs {} File'.format(snapshot_vs)
elif desired_snapshot == 'previous_quarter':
    file_name = '{}_BookingsSnapshot_Q{}_Final__{}.xlsx'.format(date_range, previous_quarter, today)
    pivot_sheet_name = 'PQ Final Sales & Churn'
    stages = ['4 - Closed', '5 - Accepted']
    target_quarter = previous_quarter
    comparison_file = 'N/A'
else:
    print('Invalid pivot date range selected, please check your syntax.')
    exit()

daily_export = local_path + '/' + file_name
payload = df.to_excel(daily_export, index = 0, sheet_name = data_sheet_name, header = True)


## Read the daily file, and prep DataFrame structures for analysis:
writer = pd.ExcelWriter(daily_export, engine = 'xlsxwriter')  # pylint: disable=abstract-class-instantiated
# Sort daily export descending by Amount, this will be the final Raw Data tab:
read_daily = pd.read_excel(daily_export, sheet_name = data_sheet_name, index_col = 0)
sort_by_amount = read_daily.sort_values(['Total_MRR_Calc__c'], ascending = False)
sort_by_amount.to_excel(writer, sheet_name = data_sheet_name)
## Filter by Stage, Forecast Category, and Close Date (for pivots by Sales & Churn for each RBG and Region):
# Stage & Forecast Category Filters
stage_filtered = read_daily[read_daily.StageName.isin(stages)]
forecast_category_filtered = stage_filtered[stage_filtered.ForecastCategoryName != 'Omitted']
# Quarter (close date) Filters
date_series = forecast_category_filtered.CloseDate
conversion = pd.to_datetime(date_series)
return_quarter = conversion.dt.quarter
date_filtered = forecast_category_filtered[return_quarter == target_quarter]
# Sales vs Churn Filters
sales_filtered = date_filtered[date_filtered.Total_MRR_Calc__c > 0.00]
churn_filtered = date_filtered[date_filtered.Total_MRR_Calc__c < 0.00]
# Sales Pivot
sales_pivot = pd.pivot_table(data = sales_filtered, values = 'Total_MRR_Calc__c', aggfunc = [np.sum, np.size], \
    index = pivot_index)
sales_pivot.to_excel(writer, sheet_name = pivot_sheet_name, startrow = 1, startcol = 0)
# Churn Pivot
churn_pivot = pd.pivot_table(data = churn_filtered, values = 'Total_MRR_Calc__c', aggfunc = [np.sum, np.size], \
    index = pivot_index)
churn_pivot.to_excel(writer, sheet_name = pivot_sheet_name, startrow = 1, startcol = 8)

if comparison_file != 'N/A':
    ## Read the WoW comparison file, and collect WoW comparison pivots (DataFrames) to compare vs daily export:
    reader = pd.read_excel(comparison_file, sheet_name = data_sheet_name)
    stage_comparison = reader[reader.StageName.isin(stages)]
    forecast_category_comparison = stage_comparison[stage_comparison.ForecastCategoryName != 'Omitted']
    date_series_comparison = forecast_category_comparison.CloseDate
    conversion_comparison = pd.to_datetime(date_series_comparison)
    return_quarter_comparison = conversion_comparison.dt.quarter
    date_comparison = forecast_category_comparison[return_quarter_comparison == target_quarter]
    sales_comparison = date_comparison[date_comparison.Total_MRR_Calc__c > 0.00]
    churn_comparison = date_comparison[date_comparison.Total_MRR_Calc__c < 0.00]
    # Sales WoW Variance   
    sc_pivot = pd.pivot_table(data = sales_comparison, values = 'Total_MRR_Calc__c', aggfunc = [np.sum, np.size], \
        index = pivot_index)
    sales_variance = sales_pivot.subtract(sc_pivot, axis = 0)
    sales_variance.to_excel(writer, sheet_name = variance_tab, startrow = 1, startcol = 0)
    # Churn WoW Variance
    cc_pivot = pd.pivot_table(data = churn_comparison, values = 'Total_MRR_Calc__c', aggfunc = [np.sum, np.size], \
        index = pivot_index)
    churn_variance = churn_pivot.subtract(cc_pivot, axis = 0)
    churn_variance.to_excel(writer, sheet_name = variance_tab, startrow = 1, startcol = 8)
    writer.save()
else:
    # No WoW Variance, skip above steps that are for CQ snapshots only
    writer.save()


## Note, user will face permission issues if trying to write files directly to Drive file stream via .bat file for task scheduling.. 
    #below block moves final file from local path to the shared Drive folder:
try:
    shutil.move(daily_export, destination)
    print('Data query, export, and archive complete')
## Note, script will error if file already exists within shared Drive folder (destination).
    # This will prevent accidental re-writes. User must manually remove the daily export to re-run the script:
except EnvironmentError as error:
    os.remove(daily_export)
    print('Daily data archive has already been run!  Please manually remove the snapshot from today, \
if a brand new snapshot is truly desired.', error)

## Note, can skip steps to write to local directory, if writing directly to the shared Drive folder without the use of a .bat file..
    # Just replace the 'local_path' line in the file handling block, and remove the try/except block, as it will not be needed.
## Link to Shared Drive folder:  https://drive.google.com/drive/u/0/folders/0AHcO-eCVun8iUk9PVA
