import pymysql


class DbServers:
	"""Database servers definition"""

	# -- constructor --
	def __init__(self):
		self.debug_mode = 1

		return

	def db_bolsa(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa"

		return [sql, curr_db]

	def db_bolsa19962000(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa19962000",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa19962000"

		return [sql, curr_db]

	def db_bolsa20012005(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa20012005",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa20012005"

		return [sql, curr_db]

	def db_bolsa20062010(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa20062010",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa20062010"

		return [sql, curr_db]

	def db_bolsa20112015(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa20112015",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa20112015"

		return [sql, curr_db]

	def db_bolsa20162020(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa20162020",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa20162020"

		return [sql, curr_db]

	def db_bolsa20212025(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="bolsa20212025",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "bolsa20212025"

		return [sql, curr_db]

	def db_statsrt(self):
		with pymysql.connect(
			host="localhost",
			user="user",
			passwd="password",
			db="statsrt",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "statsrt"

		return [sql, curr_db]

	def db_remote(self):
		with pymysql.connect(
			host="remote_server",
			user="user",
			passwd="password",
			db="bolsart",
			cursorclass=pymysql.cursors.DictCursor
		) as sql:
			curr_db = "remote"

		return [sql, curr_db]
