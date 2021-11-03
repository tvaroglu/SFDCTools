class Transformer:
    def __init__(self, opps):
        self.opps = opps
        self.valid_term_lengths = [12.0, 24.0, 36.0, 60.0]

    ## This function checks Opp vs SO financial field values. If anything doesn't align between the two, the Opp is excluded from future validation stages:
    def validate_opp_to_service_order(self, service_orders):
        output_list = []
        for o in self.opps:
            for s in service_orders:
                if o['Id'] == s['Opportunity__c']:
                    amount_ck = o['Amount'] == (s['MRR__c'] - s['Tax_Fee_Pass_Through__c'])
                    tax_ck = o['Tax_Fee_Pass_Through__c'] == s['Tax_Fee_Pass_Through__c']
                    nrr_ck = o['Installation_NRR__c'] == s['NRR__c']
                    mar_ck = o['Total_MAR__c'] == s['MAR__c']
                    term_ck = o['Term_in_Months__c'] == s['Term__c']
                    netex_mrc_ck = o['Netex_MRC_Approved__c'] == s['Netx_MRC__c']
                    netex_nrc_ck = o['Netex_NRC_Approved__c'] == s['Netx_NRC__c']
                    if amount_ck and tax_ck and nrr_ck and mar_ck and term_ck and netex_mrc_ck and netex_nrc_ck and o['Id'] not in output_list:
                        output_list.append(o['Id'])
        return output_list

    ## This function checks Opp vs Quote or COR form, based on Workflow Type. If anything doesn't align between the two, the Opp is excluded from future validation stages:
    def validate_opp_to_quote_or_cor_form(self, valid_opp_ids, quotes, cor_forms):
        output_list = []
        for v in valid_opp_ids:
            for o in self.opps:
                if v == o['Id'] and o['Opportunity_Workflow_Type__c'] in ['Standard Pricing', 'Tranzact Custom Solution', 'ASR (Automated)']:
                    for q in quotes:
                        if o['Id'] == q['Opportunity__c'] and q['CPQ_Status__c'] in ['Ordered', 'Order Pending']:
                            capex_ck = o['Total_Success_Based_Capex_Calc__c'] == q['Total_Capex__c']
                            if o['Term_in_Months__c'] == 12.0:
                                cust_mrc_ck = o['Amount'] == q['X12_MRC_Quote__c']
                                cust_nrc_ck = o['Installation_NRR__c'] == q['X12_NRR_Quote__c']
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == q['X12_Netex_MRC__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == q['X12_Netex_NRC__c']
                            elif o['Term_in_Months__c'] == 24.0:
                                cust_mrc_ck = o['Amount'] == q['X24_MRC_Quote__c']
                                cust_nrc_ck = o['Installation_NRR__c'] == q['X24_NRR_Quote__c']
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == q['X24_Netex_MRC__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == q['X24_Netex_NRC__c']
                            elif o['Term_in_Months__c'] == 36.0:
                                cust_mrc_ck = o['Amount'] == q['X36_MRC_Quote__c']
                                cust_nrc_ck = o['Installation_NRR__c'] == q['X36_NRR_Quote__c']
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == q['X36_Netex_MRC__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == q['X36_Netex_NRC__c']
                            elif o['Term_in_Months__c'] == 60.0:
                                cust_mrc_ck = o['Amount'] == q['X60_MRC_Quote__c']
                                cust_nrc_ck = o['Installation_NRR__c'] == q['X60_NRR_Quote__c']
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == q['X60_Netex_MRC__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == q['X60_Netex_NRC__c']
                            elif o['Term_in_Months__c'] not in self.valid_term_lengths:
                                continue
                            if capex_ck and cust_mrc_ck and cust_nrc_ck and netex_mrc_ck and netex_nrc_ck and v not in output_list:
                                output_list.append(v)
                elif v == o['Id'] and o['Opportunity_Workflow_Type__c'] == 'Custom Solution':
                    for c in cor_forms:
                        if o['Id'] == c['Special_Pricing_ICB__r']['Opportunity_Lookup__c']:
                            capex_ck = o['Total_Success_Based_Capex_Calc__c'] == c['Total_Success_Based_Capital__c']
                            if o['Term_in_Months__c'] == 12.0:
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == c['Expense_MRC_Yr1__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == c['Expense_NRC_Yr1__c']
                            elif o['Term_in_Months__c'] == 24.0:
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == c['Expense_MRC_Yr2__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == c['Expense_NRC_Yr2__c']
                            elif o['Term_in_Months__c'] == 36.0:
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == c['Expense_MRC_Yr3__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == c['Expense_NRC_Yr3__c']
                            elif o['Term_in_Months__c'] == 60.0:
                                netex_mrc_ck = o['Netex_MRC_Approved__c'] == c['Expense_MRC_Yr5__c']
                                netex_nrc_ck = o['Netex_NRC_Approved__c'] == c['Expense_NRC_Yr5__c']
                            elif o['Term_in_Months__c'] not in self.valid_term_lengths:
                                continue
                            if capex_ck and netex_mrc_ck and netex_nrc_ck and v not in output_list:
                                output_list.append(v)
                ## If neither Tranzact nor ICB workflow, Opp is assumed to be clean based on initial Opp vs SO check, and is added to future validation stage.
                    ## Can REMOVE these final lines, if false positives are produced as a result:
                elif v == o['Id'] and o['Opportunity_Workflow_Type__c'] == None and v not in output_list:
                    output_list.append(v)
                elif v == o['Id'] and o['Opportunity_Workflow_Type__c'] == 'Administrative' and v not in output_list:
                    output_list.append(v)
        return output_list
