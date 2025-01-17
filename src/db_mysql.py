import logging
import time
import pymysql

import db_servers
import druida_utils


srv = db_servers.DbServers()
utils = druida_utils.DruidaUtils()


class DbMysql:
    """MySql Database process class"""

    def __init__(self):
        self.debug_mode = 1
        self.logfile = ""
        self.bbdd = None
        self.sql = None
        self.table = "quotes1"
        self.database = None
        self.current_db = None
        self.date = None
        self.datehour = None
        # self.max_processes = 600
        # self.min_processes = 400
        self.max_processes = 100
        self.min_processes = 90

        return

    def set_database(self, database: str) -> None:
        self.database = database

        return

    def set_table(self, table: str) -> None:
        self.table = table

        return

    def query(self, query: str) -> tuple:
        results = tuple()
        func_name = getattr(srv, "db_" + self.database)
        self.current_db = self.database

        if self.debug_mode > 1:
            self.show_query(query)

        self.sql = func_name()
        try:
            self.sql.execute(query)
            results = self.sql.fetchall()
        except Exception as e:
            print(e)
            self.error_query(query)

        srv.bbdd.close()

        return results

    def error_query(self, query: str) -> None:
        line = "(" + self.current_db + " ERROR) " + query
        logging.info(line)
        print(line)

        return

    def insdelupd(self, query: str) -> None:
        self.current_db = self.database
        func_name = getattr(srv, "db_" + self.database)

        if self.debug_mode > 1:
            self.show_query(query)

        self.sql = func_name()
        try:
            self.sql.execute(query)
            srv.bbdd.commit()
        except pymysql.err.ProgrammingError:
            self.error_query(query)

        srv.bbdd.close()

        return

    def show_query(self, query_text: str) -> None:
        line = ("(" + self.database + ") " + query_text)
        logging.info(line)
        print(line)

        return

    def select(self, query_list: list) -> list:
        full_query = "SELECT * FROM " + self.table + " WHERE "

        for query in query_list:
            if query.upper().find("SELECT") == 0:
                full_query = query + " FROM " + self.table + " WHERE "

            if query.find("SELECT") < 0 \
                    and query.find("GROUP BY ") < 0 \
                    and query.find("ORDER BY ") < 0 \
                    and query.find("LIMIT ") < 0:
                full_query += query + " AND "

            if query.find("GROUP BY ") == 0 \
                    or query.find("ORDER BY ") == 0 \
                    or query.find("LIMIT ") == 0:
                if full_query[-5:] == " AND ":
                    full_query = full_query[0:-5]
                if full_query[-7:] == " WHERE ":
                    full_query = full_query[0:-7]
                full_query += " " + query

        if full_query[-5:] == " AND ":
            full_query = full_query[0:-5]
        if full_query[-7:] == " WHERE ":
            full_query = full_query[0:-7]

        full_query += ";"

        results = self.query(full_query)

        return list(results)

    def insert(self, query_dict: dict) -> None:
        final_query = "INSERT INTO "+self.table+" ("

        for query in query_dict.keys():
            if query != "id":
                final_query += query + ", "
        final_query = final_query[0:-2]

        final_query += ") VALUES ("

        for query_part in query_dict.keys():
            if query_part != "id":
                final_query += "'" + str(query_dict[query_part]) + "', "
        final_query = final_query[0:-2]

        final_query += ");"

        self.insdelupd(final_query)

        return

    def row_insert(self, row_list: list) -> None:
        self.current_db = self.database
        func_name = getattr(srv, "db_" + self.database)

        self.sql = func_name()
        for row in row_list:
            final_query = "INSERT INTO " + self.table + " ("

            for n in row.keys():
                if n != "id":
                    final_query += n + ","
            final_query = final_query[0:-1]

            final_query += ") VALUES ("

            for n in row.keys():
                if n != "id":
                    final_query += "'" + str(row[n]) + "',"

            final_query = final_query[0:-1]
            final_query += ");"

            if self.debug_mode > 1:
                line = final_query
                logging.info(line)
                print(line)

            self.sql.execute(final_query)

            time.sleep(0.0001)

        srv.bbdd.commit()
        srv.bbdd.close()

        return

    def delete(self, query_list: list) -> None:
        final_query = "DELETE FROM "+self.table

        if len(query_list) > 0:
            final_query += " WHERE "

            for query in query_list:
                final_query += query + " AND "
            final_query = final_query[0:-5]

        final_query += ";"

        self.insdelupd(final_query)

        return

    def update(self, query_list1: list, query_list2: list) -> None:
        if len(query_list1) > 0:
            final_query = "UPDATE "+self.table+" SET "

            for query1 in query_list1:
                final_query += str(query1) + ", "
            final_query = final_query[0:-2]

            if len(query_list2) > 0:
                final_query += " WHERE "
            for query2 in query_list2:
                final_query += str(query2) + " AND "
            final_query = final_query[0:-5]

            final_query += ";"

            self.insdelupd(final_query)

        return

    def get_bolsa_database(self, date: str) -> None:
        self.date = date
        today = time.strftime("%Y-%m-%d", time.localtime())
        self.set_database("bolsa")
        self.set_table("quotes1")

        utils.datehour = date + " 00:00:00"
        secs = utils.get_timestamp(date)

        if 820450800 <= secs <= 978303599:
            self.set_database("bolsa19962000")
        if 978303600 <= secs <= 1136069999:
            self.set_database("bolsa20012005")
        if 1136070000 <= secs <= 1293836399:
            self.set_database("bolsa20062010")
        if 1293836400 <= secs <= 1451602799:
            self.set_database("bolsa20112015")
        if 1451602800 <= secs <= 1609455599:
            self.set_database("bolsa20162020")
        if 1609455600 <= secs <= 1704063599:
            self.set_database("bolsa20212025")

        if self.debug_mode > 2:
            line = ("DATABASE selected: " + self.database)
            logging.info(line)
            print(line)

        if self.database == "bolsa":
            if self.date == today:
                self.set_table("quoteday")

        return
