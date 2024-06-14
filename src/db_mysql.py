import time
import db_servers


srv = db_servers.DbServers()


class DbMysql:
    """MySql Database process class"""

    def __init__(self):
        self.debug_mode = 2
        self.logfile = ""
        self.sql = None
        self.bbdd = "bolsa"
        self.table = "quotes1"
        self.database = None
        self.current_db = None
        self.date = None
        self.datehour = None

        return

    def open_db(self, database):
        func_name = getattr(srv, "db_"+database)
        [self.sql, self.bbdd, self.current_db] = func_name()

        return

    def query(self, query: str, db: str) -> tuple:
        results = tuple
        self.open_db(db)

        try:
            results = self.make_query(query)
        except:
            self.error_query(query)

        return results

    def make_query(self, query: str) -> tuple:
        if self.debug_mode > 1:
            line = "("+self.current_db+") "+query
            print(line)

        self.sql.execute(query)
        results = self.sql.fetchall()

        self.bbdd.close()

        return results

    def error_query(self, query: str) -> None:
        self.bbdd.close()
        line = "("+self.current_db+" ERROR) "+query
        print(line)

        return

    def insdelupd(self, query: str, db: str) -> None:
        self.open_db(db)

        try:
            self.make_commit_query(query)
        except:
            self.error_query(query)

        self.bbdd.close()

        return

    def make_commit_query(self, query: str) -> None:
        if self.debug_mode > 1:
            line = "("+self.current_db+") "+query
            print(line)

        self.sql.execute(query)
        self.bbdd.commit()
        self.sql.fetchall()

        return

    def select(self, data_list: list) -> list:
        query = "SELECT * FROM " + self.table + " WHERE "

        for data in data_list:
            if data.upper().find("SELECT") == 0:
                query = data + " FROM " + self.table + " WHERE "

            if data.find("SELECT") < 0 \
                    and data.find("GROUP BY ") < 0 \
                    and data.find("ORDER BY ") < 0 \
                    and data.find("LIMIT ") < 0:
                query += data + " AND "

            if data.find("GROUP BY ") == 0 \
                    or data.find("ORDER BY ") == 0 \
                    or data.find("LIMIT ") == 0:
                if query[-5:] == " AND ":
                    query = query[0:-5]
                if query[-7:] == " WHERE ":
                    query = query[0:-7]
                query += " " + data

        if query[-5:] == " AND ":
            query = query[0:-5]
        if query[-7:] == " WHERE ":
            query = query[0:-7]

        query += ";"

        results = self.query(query, self.database)

        return list(results)

    def insert(self, data_dict: dict, table: str, database: str) -> None:
        query = "INSERT INTO "+table+" ("

        for data in data_dict.keys():
            if data != "id":
                query += data + ", "
        query = query[0:-2]

        query += ") VALUES ("

        for data in data_dict.keys():
            if data != "id":
                query += "'" + str(data[data]) + "', "
        query = query[0:-2]

        query += ");"

        self.insdelupd(query, database)

        return

    def row_insert(self, data_list: list, table: str, database: str) -> None:
        self.open_db(database)

        for data in data_list:
            query = "INSERT INTO " + table + " ("

            for n in data.keys():
                if n != "id":
                    query += n + ","
            query = query[0:-1]

            query += ") VALUES ("

            for n in data.keys():
                if n != "id":
                    query += "'" + str(data[n]) + "',"

            query = query[0:-1]
            query += ");"

            if self.debug_mode > 1:
                print(query)

            self.sql.execute(query)

            time.sleep(0.0001)

        self.bbdd.commit()
        self.bbdd.close()

        return

    def delete(self, data_list: list, table: str, database: str) -> None:
        self.open_db(database)

        query = "DELETE FROM "+table

        if len(data_list) > 0:
            query += " WHERE "

            for data in data_list:
                query += data + " AND "
            query = query[0:-5]

        query += ";"

        self.insdelupd(query, database)

        return

    def update(self, data_list1: list, data_list2: list, table: str,
               database: str) -> None:
        self.open_db(database)

        if len(data_list1) > 0:
            query = "UPDATE "+table+" SET "

            for data in data_list1:
                query += "`" + data + "`='" + str(data_list1[data]) + "', "
            query = query[0:-2]

            if len(data_list2) > 0:
                query += " WHERE "

                for data in data_list2:
                    query += data + " AND "
                query = query[0:-5]

            query += ";"

            self.insdelupd(query, database)

        return

    def get_database(self) -> None:
        today = time.strftime("%Y-%m-%d", time.localtime())
        self.database = "bolsa"
        self.datehour = self.date + " 00:00:00"
        secs = self.get_secs()

        if 820450800 <= secs <= 978303599:
            self.database = "bolsa19962000"
        if 978303600 <= secs <= 1136069999:
            self.database = "bolsa20012005"
        if 1136070000 <= secs <= 1293836399:
            self.database = "bolsa20062010"
        if 1293836400 <= secs <= 1451602799:
            self.database = "bolsa20112015"
        if 1451602800 <= secs <= 1609455599:
            self.database = "bolsa20162020"
        if 1609455600 <= secs <= 1704063599:
            self.database = "bolsa20212025"

        if self.database == "bolsa":
            if self.date == today:
                self.table = "quoteday"

        if self.debug_mode > 2:
            print("DATABASE selected:")
            print(self.database)

        return

    def get_secs(self) -> int:
        newhour = ""

        tmp = self.datehour.split(" ")
        date = tmp[0]
        if len(tmp) > 0:
            if len(tmp[1]) < 8:
                newhour = "0" + tmp[0] + ":" + tmp[1] + ":" + tmp[2]
            else:
                newhour = tmp[1]

        tmpsecs = time.strptime(date + " " + newhour, "%Y-%m-%d %H:%M:%S")
        secs = time.mktime(tmpsecs)

        return int(secs)
