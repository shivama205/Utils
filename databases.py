from Database import Database

def get_instance(host, user, password, db_name):
    """
    Create an instance of database with provided credentials
    host: String        Name of database host
    user: String        Database username 
    password: String    Password for user
    db_name: String     Name of database 

    Returns database instance if successful
    """
    try:
        database = Database(host, user, password, db_name)
        database.connect()
        return database
    except Exception, e:
        raise e 

if __name__ == "__main__":
    print "Using database.py"