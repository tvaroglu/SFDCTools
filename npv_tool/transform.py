class Transformer:
    def __init__(self):
        pass
    ## This function checks Opp vs SO financial field values. If anything doesn't align between the two, the Opp is excluded from future validation stages:
    def validate_opp_to_service_order(self, opps, service_orders):
        output_list = []
        for o in opps:
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
