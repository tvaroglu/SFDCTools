class Transformer:
    def __init__(self):
        pass
    ## This function checks Opp vs SO financial field values. If anything doesn't align between the two, the Opp is excluded from future validation stages:
    def validate_opp_to_service_order(self, opps, service_orders):
        output_list = []
        for o in opps:
            opp_id = o['Id']
            opp_amount = o['Amount']
            opp_tax = o['Tax_Fee_Pass_Through__c']
            opp_nrr = o['Installation_NRR__c']
            opp_mar = o['Total_MAR__c']
            opp_term = o['Term_in_Months__c']
            opp_netex_mrc = o['Netex_MRC_Approved__c']
            opp_netex_nrc = o['Netex_NRC_Approved__c']
            for s in service_orders:
                so_raw_mrr = s['MRR__c'] - s['Tax_Fee_Pass_Through__c']
                so_tax = s['Tax_Fee_Pass_Through__c']
                so_nrr = s['NRR__c']
                so_mar = s['MAR__c']
                so_term = s['Term__c']
                so_netex_mrc = s['Netx_MRC__c']
                so_netex_nrc = s['Netx_NRC__c']
                if opp_id == s['Opportunity__c']:
                    amount_ck = opp_amount == so_raw_mrr
                    tax_ck = opp_tax == so_tax
                    nrr_ck = opp_nrr == so_nrr
                    mar_ck = opp_mar == so_mar
                    term_ck = opp_term == so_term
                    netex_mrc_ck = opp_netex_mrc == so_netex_mrc
                    netex_nrc_ck = opp_netex_nrc == so_netex_nrc
                    if amount_ck and tax_ck and nrr_ck and mar_ck and term_ck and netex_mrc_ck and netex_nrc_ck and opp_id not in output_list:
                        output_list.append(opp_id)
        return output_list
