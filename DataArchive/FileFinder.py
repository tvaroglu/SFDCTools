import sys
import os

import pprint
pp = pprint.PrettyPrinter(indent=2)

from datetime import date, datetime, timedelta
today = date.today()
import datetime as dt



## From DataArchive module
date_range = 'THIS_YEAR'
# Directory info for file retrieval:
file_name = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, today)
#destination = 'C:/Users/tvaroglu/Desktop/Product Dev/Coding/Testing/DataArchive/DateRanges'
destination = 'K:/Shared drives/WAN-Eth_FunnelDataArchive'


def retrieveWoWFile (path):
    #global target_file
    path = destination
    # WoW comparison:
    files = []
    for file in os.listdir(path):
        name_split = file.partition('__')
        dated_section = name_split[-1]
        date_split = dated_section.partition('.xlsx')
        anchor_dates = date_split[0]
        files.append(anchor_dates)
    # Year logic:
    target_year = '2020'
        #Backlog item, build in year/month logic for December pulls
    # Month logic:
    if today.month == 1:
        curr_month = '01'
        prev_month = '12'
    elif today.month == 10:
        curr_month = '10'
        prev_month = '09'
    elif today.month < 10 and today.month > 1:
        curr_month = '0{}'.format(today.month)
        prev_month = '0{}'.format(today.month - 1)
    else:
        curr_month = str(today.month)
        prev_month = str(today.month - 1)
    if curr_month in ['01', '05', '07', '10', '12']:
        curr_days = 31  # pylint: disable=unused-variable
        prev_days = 30
    elif curr_month == '02':
        curr_days = 28
        prev_days = 31
    elif curr_month == '03':
        curr_days = 31
        prev_days = 28
    elif curr_month == '08':
        curr_days = 31
        prev_days = 31
    else:
        curr_days = 30
        prev_days = 31
    if today.day < 8:
        target_month = prev_month
    else:
        target_month = curr_month
    # Day logic:
    if target_month == curr_month:
        target_day = today.day - 7
        if target_day < 10:
            target_day = '0{}'.format(str(target_day))
        else:
            target_day = str(target_day)
    elif target_month == prev_month:
        delta = 7 - (today.day - 0)
        target_day = str(prev_days - delta)
    # Final file retrieval:
    target_date = '{}-{}-{}'.format(target_year, target_month, target_day)
    if target_date not in files:
        next_day = (int(target_day) + 1)
        if next_day > prev_days:
            target_month = curr_month
            target_day = next_day - prev_days
            if target_day < 10:
                next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(target_day)))
            else:
                next_target = '{}-{}-{}'.format(target_year, target_month, str(target_day))
        else:
            if next_day < 10:
                next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(next_day)))
            else:
                next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
        if next_target in files:
            target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
        else:
            next_day = (int(target_day) - 1)
            if next_day < 1:
                next_target = '{}-{}-{}'.format(target_year, prev_month, prev_days + next_day)
            else:
                next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
            if next_target in files:
                target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
            else:
                next_day = (int(target_day) + 2)
                if next_day > prev_days:
                    target_month = curr_month
                    target_day = next_day - prev_days
                    if target_day < 10:
                        next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(target_day)))
                    else:
                        next_target = '{}-{}-{}'.format(target_year, target_month, str(target_day))
                else:
                    if next_day < 10:
                        next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(next_day)))
                    else:
                        next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
                if next_target in files:
                    target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
                else:
                    next_day = (int(target_day) - 2)
                    if next_day < 1:
                        next_target = '{}-{}-{}'.format(target_year, prev_month, prev_days + next_day)
                    else:
                        next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
                    if next_target in files:
                        target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
                    else:
                        next_day = (int(target_day) + 3)
                        if next_day > prev_days:
                            target_month = curr_month
                            target_day = next_day - prev_days
                            if target_day < 10:
                                next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(target_day)))
                            else:
                                next_target = '{}-{}-{}'.format(target_year, target_month, str(target_day))
                        else:
                            if next_day < 10:
                                next_target = '{}-{}-{}'.format(target_year, target_month, '0{}'.format(str(next_day)))
                            else:
                                next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
                        if next_target in files:
                            target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
                        else:
                            next_day = (int(target_day) - 3)
                            if next_day < 1:
                                next_target = '{}-{}-{}'.format(target_year, prev_month, prev_days + next_day)
                            else:
                                next_target = '{}-{}-{}'.format(target_year, target_month, str(next_day))
                            if next_target in files:
                                target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, next_target)
                            else:
                                target_file = file_name
    else:
        next_day = 'N/A'
        next_target = 'N/A'
        target_file = '{}_FunnelSnapshot__{}.xlsx'.format(date_range, target_date)
    to_retrieve = path + '/' + target_file
    return to_retrieve

#print(retrieveWoWFile(destination))