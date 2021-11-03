import pprint
pp = pprint.PrettyPrinter(indent=2)
import shutil
import os
import json

from extract import Extractor
from transform import Transformer
from load import Loader
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

    npv_task_query = e.get_npv_task_info(formatted_opp_ids)
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

target_dict_list = final_dict_list if full_soql_query_mode else envdata

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
valid_opp_to_quote_or_cor_form = t.validate_opp_to_quote_or_cor_form(valid_opp_to_service_orders)
standardized_opp_to_cp_or_eb = t.standardize_opp_to_cp_or_eb(valid_opp_to_quote_or_cor_form)
valid_opp_to_cp_or_eb = t.validate_opp_to_cp_or_eb(valid_opp_to_quote_or_cor_form, standardized_opp_to_cp_or_eb)
print(valid_opp_to_cp_or_eb)

## All validation stages passed, applicable NPV tasks can now be closed:
l = Loader(valid_opp_to_cp_or_eb, npv_tasks)
tasks_closed = l.load_tasks() if full_soql_query_mode else []
if len(tasks_closed) == 0:
    print('0 NPV tasks validated by automation.')
elif len(tasks_closed) > 0:
    print('{} NPV tasks validated by automation. Please move to Stage 5 via the corresponding report queue: \n \
        https://zayo.my.salesforce.com/00O0z000005btK4'.format(len(tasks_closed)))
