class Loader:
    def __init__(self, valid_opp_ids, npv_tasks):
        self.valid_opp_ids = valid_opp_ids
        self.npv_tasks = npv_tasks
        self.task_attributes = {
            'Status': 'Completed',
            'OwnerId': '00560000001hYAUAA2',
            'Additional_Task_Response_Notes__c': 'Completed via Automation'
        }

    def load_tasks(self):
        output_list = []
        for v in self.valid_opp_ids:
            for n in self.npv_tasks:
                if v == n['WhatId'] and n['Id'] not in output_list:
                    output_list.append(n['Id'])
        for task in output_list:
            result = sf.Task.update(task, self.task_attributes)
        return output_list
