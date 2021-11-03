import pprint
pp = pprint.PrettyPrinter(indent=2)
import csv
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from simple_salesforce import Salesforce, SalesforceLogin
from pandas import DataFrame
import pandas as pd
import numpy as np

from DataArchive.auth import bulk
from DataArchive.auth import sf

## SFDC API creds stored separately, for masking purposes. Ref syntax below:
#bulk = SalesforceBulk(username=username, password=password, security_token=token)
#sf = Salesforce(username=username, password=password, security_token=token, client_id='Testing',
    #instance_url="https://zayo.my.salesforce.com", session_id='')


## Modify SF object as desired. Note, API names are required:
# reference link to SF Objects page for API names:  https://zayo.my.salesforce.com/p/setup/custent/CustomObjectsPage?setupid=CustomObjects&retURL=%2Fui%2Fsetup%2FSetup%3Fsetupid%3DDevTools
sf_object = 'Opportunity'
#sf_object = 'Service_Order__c'
#sf_object = 'Service_Capability__c'
#sf_object = 'Task'

## Specify job type (update or insert) as follows:
job_type = 'update'
#job_type = 'insert'

## Specify spreadsheet as follows:
spreadsheet_to_run = 'SepQ20_AmountVariance_10.2.20'

## Specify (your) UserID for SOQL query in UnitTest:
user_id = '00560000001hYAUAA2'


## Create the bulk API job, and run the bulk upload. CSV file must be in same dir as Python script, unless otherwise specified:
reader = csv.DictReader(open('{}.csv'.format(spreadsheet_to_run)))
disbursals = []

for row in reader:
    disbursals.append(row)
total_size = np.asarray(disbursals)
if len(disbursals) < 10:
    batch_size = len(disbursals)
else:
    batch_size = 10
batches = np.array_split(total_size, batch_size)
for b in batches:
    if job_type == 'update':
        job = bulk.create_update_job('{}'.format(sf_object), contentType='CSV', concurrency='Parallel')
    elif job_type == 'insert':
        job = bulk.create_insert_job('{}'.format(sf_object), contentType='CSV', concurrency='Parallel')
    else:
        print('Invalid job type specified, please check your syntax.')
        exit()
    csv_iter = CsvDictsAdapter(iter(b))
    batch = bulk.post_batch(job, csv_iter)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    results = bulk.get_batch_results(batch, job)
    for r in results:
        print(r)
print('{} performed for {} total {} records.'.format(job_type,len(disbursals),sf_object))


## UnitTest to validate data uploaded. Note, multiple uploads on the same object in one day can trigger a false positive for a failed test (will print negative variance):
field_list = 'Id'
field_query = field_list.strip("'")
soql_data = sf.query_all("SELECT {} from {} Where LastModifiedById = '{}' and SystemModStamp = TODAY".format(field_query, sf_object, user_id))
df = pd.DataFrame(soql_data['records']).drop(columns='attributes')

if len(df) == len(disbursals):
    print('{} SUCCESSFUL in SFDC for {} total {} records, your data was clean.'.format(job_type, len(disbursals), sf_object))
else:
    variance = len(disbursals) - len(df)
    print('{} {} records were UNSUCCESSFUL, please validate any errors and re-run your {}.'.format(variance, sf_object, job_type))



### Note, per Salesforce Dev documentation.. failed records return empty string for ID:  https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_batches_failed_records.htm
### Salesforce knowledge article with same info as above:  https://help.salesforce.com/articleView?id=000349496&language=en_US&mode=1&type=1

### Reference for Workbench/APEX Rest URI Calls:  https://rajvakati.com/2018/03/29/salesforce-bulk-api-2-0/
### Trailhead module:  https://trailhead.salesforce.com/en/content/learn/modules/api_basics/api_basics_bulk

### Date formatting:  https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_date_format.htm
