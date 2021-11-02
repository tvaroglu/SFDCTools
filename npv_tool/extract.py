class Extractor:
    def __init__(self):
        pass
    ### ALL SOQL DATA QUERY PARAMETERS SPECIFIED WITHIN THIS CLASS:
    def get_opp_info(self):
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

    def format_opp_ids(self, opps, size):
        stringers = ''
        for x in range(0,size-1):
            stringers = "'" + opps['records'][x]['Id'] + "'," + stringers
        stringers = stringers + "'" + opps['records'][size-1]['Id'] + "'"
        return stringers
