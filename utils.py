import MySQLdb
from time import sleep

def is_set(variable):
	"""
	Check if string is None/empty or not
	variable: String 		Variable to be checked

	Returns true if variable is set else returns false
	"""
	try:
		if variable:
			return True
		else:
			return False
	except Exception, e:
		raise e

def is_string_type(variable):
	"""
	Check if variable is of type string or not
	variable: String 		Variable to be checked 

	Returns true if varaible is of type string else returns false
	"""
	try:
		if isinstance(variable, str) or isinstance(variable, unicode):
			return True
		else:
			return False
	except Exception, e:
		raise e

def establish_db_connection(host, user, password, db_name, retry=3, sleep_duration=1):
	"""
	Try to establish connection to database for retry number of times
	retry: int 			Number of retry to perform	(Optional)

	Returns database connection object on success
	"""
	try:
		while(retry):
			try:
				return MySQLdb.connect(host, user, password, db_name)
			except:
				sleep(sleep_duration)
				retry -= 1
		raise Exception("Can't connect to mysql host: " + host + " using user: " + user)
	except Exception, e:
		raise e
