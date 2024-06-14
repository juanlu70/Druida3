#!/usr/bin/env python3

import argparse
import time

import train_matchs
import train_utils
import db_mysql
import libstats


tu = train_utils.TrainUtils()
db = db_mysql.DbMysql()


parser = argparse.ArgumentParser()
parser.add_argument("-t", "--ticker", type=str, default='KR-XBTUSD', help="Ticker symbol")
parser.add_argument("-i", "--date_ini", type=str, default="", help="Begin date")
parser.add_argument("-f", "--date_end", type=str, default="", help="End date")
parser.add_argument("-d", "--delete", action="count", default=0, help="Delete data between dates")
parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity level")
args = parser.parse_args()

secs_ini = 0
secs_end = 0

tokens = {
    "ticker": args.ticker,
    "date_ini": args.date_ini,
    "secs_ini": secs_ini,
    "date_end": args.date_end,
    "secs_end": secs_end,
    "delete": args.delete,
    "verbose": args.verbose
}

if tokens['ticker'] == "":
    tokens['ticker'] = "KR-XBTUSD"
    ticker = tokens['ticker']

if tokens['date_ini'] == "":

    db.date = tokens['date_ini']
    db.get_database()

    params = [
        "SELECT MAX(fecha)",
        "ticker='"+tokens['ticker']+"'"
    ]
    tmp = db.select(params, "match_close", "statsrt")
    tokens['date_ini'] = tu.calc_dates(str(tmp[0]['MAX(fecha)']), 1)[0]
    tokens['secs_ini'] = tu.calc_dates(str(tmp[0]['MAX(fecha)']), 1)[1]
else:
    if tokens['secs_ini'] == 0:
        tokens['secs_ini'] = tu.calc_dates(tokens['date_ini'], 0)[1]

if tokens['date_end'] == "":
    tmp = time.strftime("%Y-%m-%d", time.localtime())
    tokens['date_end'] = tu.calc_dates(tmp, -1)[0]
    tokens['secs_end'] = tu.calc_dates(tmp, -1)[1]
else:
    if tokens['secs_end'] == 0:
        tokens['secs_end'] = tu.calc_dates(tokens['date_end'], 0)[1]

# -- print values if debug activated --
if tokens['verbose'] >= 1:
    db.debug_mode = 2
    print("TOKENS:")
    print(tokens)

return tokens


# #########################################################################
# # MAIN
if __name__ == "__main__":
    tr = Training()

    # -- process arguments --
    argums = tr.arguments()
    tr.ticker = argums['ticker']

    # # -- if delete --
    # if argums['delete'] == 1:
    #     params = ["ticker='"+argums['ticker']+"'"]
    #     db.delete(params, "match")

    # -- get dates --
    print("*** GET DIFFERENT DATES...")
    database = db.get_database(argums['secs_ini'])
    params = [
        "SELECT DISTINCT(fecha)",
        "ticker='"+argums['ticker']+"'",
        "secs>='"+str(argums['secs_ini'])+"'",
        "secs<='"+str(argums['secs_end'])+"'",
        "ORDER BY fecha"
    ]
    db_dates = db.select(params, "quotes1", database)

    # -- launch parallel training by day --
    print("*** GET DAY DATA...")
    proc_dates = {}
    days_data = {}
    day_count = 0
    last_day_count = 0
    procs = []

    for day in db_dates:
        proc_dates['curr_date'] = tu.calc_dates(str(day['fecha']), 0)[0]
        proc_dates['l_day'] = tu.calc_dates(str(day['fecha']), -1)[0]
        proc_dates['ll_day'] = tu.calc_dates(str(day['fecha']), -2)[0]

        # -- get day data --
        if last_day_count >= 2:
            days_data['ll_day'] = days_data['l_day']
            days_data['l_day'] = days_data['curr_date']

            params = [
                "SELECT *",
                "ticker='"+argums['ticker']+"'",
                "fecha ='" + proc_dates['curr_date'] + "'",
                "ORDER BY hora"
            ]
            days_data['curr_date'] = db.select(params, "quotes1", database)
        else:
            for item in proc_dates.keys():
                params = [
                    "SELECT *",
                    "ticker='"+argums['ticker']+"'",
                    "fecha ='" + proc_dates[item] + "'",
                    "ORDER BY hora"
                ]
                days_data[item] = db.select(params, "quotes1", database)
        last_day_count += 1
        day_count += 1

        # -- launch parallel process and wait for each to finish before process next day --
        procs = []

        for process in tr.processes:
            # if process == "main_maxmin" \
            #         or process == "ld_close" \
            #         or process == "ld_open" \
            #         or process == "ld_high" \
            #         or process == "ld_low":
            # if process == "ld_currlow":
            func_name = getattr(tm, process)
            # -- execute in multiprocessing --
            p = mp.Process(target=func_name, args=[argums['ticker'], days_data, proc_dates['curr_date']])
            procs.append(p)
            p.start()
            # -- don't execute in multiprocessing --
            # func_name(argums['ticker'], days_data, proc_dates['curr_date'])

        for prcs in procs:
            prcs.join(timeout=0)
            if prcs.is_alive():
                time.sleep(1)

        time.sleep(000.1)
        print("----------------------------------------------------------------------------------")
