#!/usr/bin/env python3

import argparse
import time

import druida_base


dr = druida_base.DruidaBase()


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticker", type=str, default='KR-XBTUSD', help="Ticker symbol")
parser.add_argument("-d", "--date", type=str, default=time.strftime("%Y-%m-%d", time.localtime()),
                    help="Date")
parser.add_argument("-g", "--trading", action="count", default=1, help="Disable trading mode")
parser.add_argument("-l", "--log_file", type=str, default="", help="Specify log file")
parser.add_argument("-v", "--verbose", action="count", default=0,
                    help="Increase verbosity level (values: 0-1-2)")
parser.add_argument("-o", "--output", type=str, default="screen",
                    help="Output mode (values: screen, file, database)")
args = parser.parse_args()

arguments = {
    "ticker": args.ticker,
    "date": args.date,
    "trading": args.trading,
    "log_file": args.log_file,
    "verbose": args.verbose
}

if arguments['log_file'] == "":
    arguments['log_file'] = "logD3-" + arguments['ticker'] + "-" + arguments['date'] + ".log"

if arguments['trading'] > 1:
    arguments['trading'] = 0

if arguments['date'] == "":
    arguments['date'] = time.strftime("%Y-%m-$d", time.localtime())

if arguments['verbose'] >= 1:
    print("ARGUMENTS:")
    print(arguments)

dr.set_arguments(arguments)
dr.make_previous_db_clean()
dr.start_logging()
dr.run_druida_process_old_data()
while 0 == 0:
    dr.run_druida_process_current_data()
