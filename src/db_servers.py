import pymysql


class DbServers:
	"""Database servers definition"""

	# -- constructor --
	def __init__(self):
		self.debug_mode = 1

		return

	def db_bolsa(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa"

		return [sql, bbdd, curr_db]

	def db_bolsa19962000(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa19962000")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa19962000"

		return [sql, bbdd, curr_db]

	def db_bolsa20012005(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20012005")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20012005"

		return [sql, bbdd, curr_db]

	def db_bolsa20062010(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20062010")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20062010"

		return [sql, bbdd, curr_db]

	def db_bolsa20112015(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20112015")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20112015"

		return [sql, bbdd, curr_db]

	def db_bolsa20162020(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20162020")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20162020"

		return [sql, bbdd, curr_db]

	def db_bolsa20212025(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20212025")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20212025"

		return [sql, bbdd, curr_db]

	def db_statsrt(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "statsrt")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "statsrt"

		return [sql, bbdd, curr_db]

	def db_remote(self):
		bbdd = pymysql.connect(host = "mulderbot.no-ip.org", user = "juanlu", passwd = "", db = "bolsart")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "remote"

		return [sql, bbdd, curr_db]


