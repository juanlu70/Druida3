#!/usr/bin/env python3

import argparse
import time

import druida_data
import druida_utils
import training_base


data = druida_data.DruidaData()
utils = druida_utils.DruidaUtils()
tb = training_base.TrainingBase()


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticker", type=str, default='KR-XBTUSD', help="Ticker symbol")
parser.add_argument("-i", "--date_ini", type=str, default="", help="Begin date")
parser.add_argument("-f", "--date_end", type=str, default="", help="End date")
parser.add_argument("-d", "--delete", action="count", default=0, help="Delete data between dates")
parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity level")
args = parser.parse_args()

secs_ini = 0
secs_end = 0

arguments = {
    "ticker": args.ticker,
    "date_ini": args.date_ini,
    "secs_ini": secs_ini,
    "date_end": args.date_end,
    "secs_end": secs_end,
    "delete": args.delete,
    "verbose": args.verbose
}

if arguments['ticker'] == "":
    arguments['ticker'] = "KR-XBTUSD"

if arguments['date_ini'] == "":
    data.set_arguments(arguments)

    print("Getting last date registered in training DB...")
    max_secs = data.get_max_date()

    if max_secs > 0:
        tmp = time.strftime("%Y-%m-%d", time.localtime(max_secs))
        arguments['date_ini'] = utils.calculate_dates(tmp, 1)[0]
    else:
        arguments['date_ini'] = "2016-01-01 00:00:00"
        arguments['date_end'] = "2016-12-31 23:59:59"
        arguments['secs_end'] = utils.get_timestamp(arguments['date_end'])

    arguments['secs_ini'] = utils.get_timestamp(arguments['date_ini'] + " 00:00:00")

if arguments['date_end'] == "":
    today = time.strftime("%Y-%m-%d", time.localtime())
    [arguments['date_end'], arguments['secs_end']] = utils.calculate_dates(today, -1)

if arguments['verbose'] >= 1:
    print("(arguments) "+str(arguments))

tb.arguments = arguments
tb.run_training()
