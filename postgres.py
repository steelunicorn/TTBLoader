import psycopg2 as pg
from psycopg2.extras import execute_values, RealDictCursor
import logging
from config import APP_NAME


class Postgres:
	conn = None
	keep = True

	def __init__(self, _host, _user, _appname='Generic Python App'):
		_password = _user
		self.logger = logging.getLogger(APP_NAME)
		self._db_config = {'host': _host, 'user': _user, 'password': _password, 'dbname': 'gdbase', 'application_name': _appname+' ['+_user+']'}
		self.connect()

	def __enter__(self):
		return self

	def set_autocommit(self, value):
		self.connect()
		self.conn.autocommit = value

	def set_keepconnection(self, value):
		self.keep = value
		if not self.keep:
			self.conn.close()

	def connect(self):
		if self.conn is None or self.conn.closed != 0:
			try:
				self.conn = pg.connect(**self._db_config)
				self.conn.autocommit = True
			except Exception as err:
				self.logger.error('Error connecting to database: {}'.format(err))
				raise err

	def query(self, _query, _params=None):
		self.connect()
		try:
			with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
				cursor.execute(_query, _params)
				return cursor.fetchall() if cursor.description else cursor.rowcount
		except Exception as error:
			self.logger.error('Error executing query {}, error: {}'.format(_query, error))
			raise error
		finally:
			if not self.keep:
				self.conn.close()

	def batch_insert(self, _query, _values, _template=None, _page_size=10000):
		self.connect()
		try:
			with self.conn.cursor() as cursor:
				execute_values(cursor, _query, _values, _template, _page_size)
				return cursor.fetchall() if cursor.description else cursor.rowcount

		except Exception as error:
			self.logger.error('Error executing query {}, error: {}'.format(_query, error))
			raise error

	def __exit__(self, exc_type, exc_val, exc_tb):
		if self.conn:
			self.conn.close()
