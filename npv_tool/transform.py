class Transformer:
    def __init__(self, opps, service_orders, quotes, cor_forms, cap_projects, expense_builders):
        self.opps = opps
        self.service_orders = service_orders
        self.quotes = quotes
        self.cor_forms = cor_forms
        self.cap_projects = cap_projects
        self.expense_builders = expense_builders
        self.valid_term_lengths = [12.0, 24.0, 36.0, 60.0]

    ## This function checks Opp vs SO financial field values. If anything doesn't align between the two, the Opp is excluded from future validation stages:
    def validate_opp_to_service_order(self):
        output_list = []
        for o in self.opps:
            for s in self.service_orders:
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
    def validate_opp_to_quote_or_cor_form(self, valid_opp_ids):
        output_list = []
        for v in valid_opp_ids:
            for o in self.opps:
                if v == o['Id'] and o['Opportunity_Workflow_Type__c'] in ['Standard Pricing', 'Tranzact Custom Solution', 'ASR (Automated)']:
                    for q in self.quotes:
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
                    for c in self.cor_forms:
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

    ## This function checks Opp vs EB & CP values, as applicable. If this stage is passed, the associated NPV task can be closed:
    def validate_opp_to_cp_or_eb(self, valid_opp_ids):
        cp_opp_dict = {}
        eb_opp_dict = {}
        for p in self.cap_projects:
            cp_opp_dict[p['Opportunity__c']] = p['ICB_Approved_Amount__c']
        for v in valid_opp_ids:
            total_netex_mrc = 0
            total_netex_nrc = 0
            for e in self.expense_builders:
                if v == e['Opportunity__c']:
                    total_netex_mrc += e['Netex_MRC__c']
                    total_netex_nrc += e['Netex_NRC__c']
                    eb_opp_dict[v] = [total_netex_mrc, total_netex_nrc]
        for v in valid_opp_ids:
            for o in self.opps:
                if v == o['Id']:
                    for p in cp_opp_dict:
                        related_opp = p
                        capex = cp_opp_dict[p]
                        if o['Id'] == related_opp:
                            capex_ck = o['Total_Success_Based_Capex_Calc__c'] == capex
                            if capex_ck:
                                cp_opp_dict[p] = 'Clean'
                            else:
                                cp_opp_dict[p] = 'Dirty'
                    for b in eb_opp_dict:
                        related_opp = b
                        total_netex_mrc = eb_opp_dict[b][0]
                        total_netex_nrc = eb_opp_dict[b][1]
                        if o['Id'] == related_opp:
                            mrc_check = o['Netex_MRC_Approved__c'] == total_netex_mrc
                            nrc_check = o['Netex_NRC_Approved__c'] == total_netex_nrc
                            if mrc_check and nrc_check:
                                eb_opp_dict[b] = 'Clean'
                            else:
                                eb_opp_dict[b] = 'Dirty'
        output_list = []
        for v in valid_opp_ids:
            ## If there is no CP or EB, assumed clean, because already validated in prior two stages.
            # Remove this block if false positives are produced:
            if v not in cp_opp_dict.keys() and v not in eb_opp_dict.keys() and v not in output_list:
                output_list.append(v)
            ## If prior two validation stages passed, and NOT 'Dirty' from CP check:
            elif v in cp_opp_dict.keys() and v not in eb_opp_dict.keys():
                for p in cp_opp_dict:
                    if v == p and cp_opp_dict[p] == 'Clean' and v not in output_list:
                        output_list.append(v)
                ## If prior two validation stages passed, and NOT 'Dirty' from EB check:
            elif v not in cp_opp_dict.keys() and v in eb_opp_dict.keys():
                for e in eb_opp_dict:
                    if v == e and eb_opp_dict[e] == 'Clean' and v not in output_list:
                        output_list.append(v)
                ## If prior two validation stages passed, and NOT 'Dirty' from both CP AND EB check:
            elif v in cp_opp_dict.keys() and v in eb_opp_dict.keys():
                for p in cp_opp_dict:
                    cap_ck = False
                    if v == p and cp_opp_dict[p] == 'Clean':
                        cap_ck = True
                    for e in eb_opp_dict:
                        eb_ck = False
                        if v == e and eb_opp_dict[e] == 'Clean':
                            eb_ck = True
                        if cap_ck and eb_ck and v not in output_list:
                            output_list.append(v)
        return output_list
