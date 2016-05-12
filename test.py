import database

if __name__ == "__main__":
	try:
		db = database.get_instance(YOUR_DB_HOST, YOUR_DB_USER, YOUR_DB_PASSWORD, YOUR_DB_NAME)
		db.get_columns(YOUT_TABLE_NAME, datatype=dict)
	except Exception, e:
		print str(e)
		exit

