import MySQLdb 
import utils
import _Config

#### TO-DO: Handle specific exceptions ####             
class Database(object):

    def __init__(self, host, user, password, db_name):
        """
        Initialize instance of database with provided credentials
        host: String        Name of database host
        user: String        Database username 
        password: String    Password for user
        db_name: String     Name of database 
        """
        try:
            self.__set_hostname(host)
            self.__set_username(user)
            self.__set_user_password(password)
            self.__set_db_name(db_name)
        except Exception, e:
            raise e

    def __get_hostname(self):
        """
        Returns hostname of Database
        """
        try:
            if self.__DB_HOST:
                return self.__DB_HOST
            else:
                raise Exception("Database host not set")
        except Exception, e:
            raise e

    def __get_username(self):
        """
        Returns name of user of Database
        """
        try:
            if self.__DB_USER:
                return self.__DB_USER
            else:
                raise Exception("Database username not set")
        except Exception, e:
            raise e

    def __get_user_password(self):
        """
        Returns password for user of Database
        """
        try:
            if self.__DB_PASSWORD:
                return self.__DB_PASSWORD
            else:
                raise Exception("Database user password not set")
        except Exception, e:
            raise e

    def __get_db_name(self):
        """
        Returns name of Database
        """
        try:
            if self.__DB_NAME:
                return self.__DB_NAME
            else:
                raise Exception("Database name not set")
        except Exception, e:
            raise e

    def get_db(self):
        """
        Returns instance of Database
        """
        try:
            if self.__DB:
                return self.__DB
            else:
                raise Exception("Database instance not set")
        except Exception, e:
            raise e

    def __set_hostname(self, hostname):
        """
        Set hostname of database
        hostname: String    Name of host of Database
        """
        try:
            if utils.is_string_type(hostname):
                self.__DB_HOST = hostname
            else:
                raise Exception("Invalid conversion of type to String, found {type_of_variable}".format(type_of_variable = type(hostname)))
        except Exception, e:
            raise e

    def __set_username(self, username):
        """
        Set name of user of database
        username: String    Name of user of Database
        """
        try:
            if utils.is_string_type(username):
                self.__DB_USER = username
            else:
                raise Exception("Invalid conversion of type to String, found {type_of_variable}".format(type_of_variable = type(username)))
        except Exception, e:
            raise e

    def __set_user_password(self, password):
        """
        Set password for user of database
        password: String    Password for user of Database
        """
        try:
            if utils.is_string_type(password):
                self.__DB_PASSWORD = password
            else:
                raise Exception("Invalid conversion of type to String, found {type_of_variable}".format(type_of_variable = type(password)))
        except Exception, e:
            raise e

    def __set_db_name(self, db_name):
        """
        Set name of database
        db_name: String     Name of Database
        """
        try:
            if utils.is_string_type(db_name):
                self.__DB_NAME = db_name
            else:
                raise Exception("Invalid conversion of type to String, found {type_of_variable}".format(type_of_variable = type(db_name)))
        except Exception, e:
            raise e

    def __set_db(self, db_instance):
        """
        Set instance of database
        db_instance: String     DB instace object
        """
        try:
            if db_instance:
                self.__DB = db_instance
            else:
                raise Exception("Invalid conversion of type to Object, found {type_of_variable}".format(type_of_variable = type(db_name)))
        except Exception, e:
            raise e

    def connect(self):
        """
        Connect to database 
        """
        try:
            self.__set_db(utils.establish_db_connection(self.__get_hostname(), self.__get_username(), self.__get_user_password(), self.__get_db_name()))
        except Exception, e:
            raise e

    def __create_cursor(self, cursorClass=_Config.default_cursor_class):
        """
        Creates cursor instance 
        cursorClass: Class          Class of cursor to be created   (Optional)
        Returns cursor instance
        """
        try:
            return self.get_db().cursor(cursorClass)
        except Exception, e:
            raise e

    def __set_autocommit(self, autocommit):
        """
        Set autocommit on/off
        autocommit: Boolean     Value of autocommit to be set
        """
        try:
            self.get_db().autocommit(autocommit)
        except Exception, e:
            raise e

    def __convert_resultset_based_on_datatype(self, resultset, datatype=None):
        """
        Returns resultset based on datatype specified
        resultset
        datatype: Class (Primitive Types)   Class of datatype required  (Optional)
        """
        try:
            if not resultset:
                return None

            if datatype == dict:
                return [result for result in resultset]
            else:
                if len(resultset[0]) == 1:
                    list_resultset = [result[0] for result in resultset if len(result) == 1]
                else:
                    list_resultset = [tuple([r for r in result]) for result in resultset]

                if datatype == list:
                    return list_resultset
                elif datatype == tuple:
                    return tuple(list_resultset)
                else:
                    return tuple(list_resultset)
        except Exception, e:
            raise e

    def execute_query(self, query, autocommit=_Config.default_autocommit, datatype=None):
        """
        Executes query on db instance
        query: String           Query to be executed
        autocommit: Boolean     Flag for autocommit     (Optional)
        datatype: Class         Class for cursor        (Optional)
        """
        try:
            self.__set_autocommit(autocommit)
            cursorClass = self.__get_cursor_class_based_on_datatype(datatype)
            cursor = self.__create_cursor(cursorClass=cursorClass)
            cursor.execute(query)
            # return cursor.fetchall()
            return self.__convert_resultset_based_on_datatype(cursor.fetchall(), datatype)
        except Exception, e:
            raise e

    def __get_cursor_class_based_on_datatype(self, datatype):
        """
        Returns cursor class based on datatype specified
        datatype: Class (Primitive Types)   Class of datatype required
        """
        try:
            if datatype == dict:
                cursorClass = MySQLdb.cursors.DictCursor
            elif datatype == tuple:
                cursorClass = MySQLdb.cursors.Cursor
            elif datatype == list:
                cursorClass = MySQLdb.cursors.Cursor
            else:
                cursorClass = _Config.default_cursor_class
            return cursorClass
        except Exception, e:
            raise e

    def get_columns(self, tablename, dbname=None, datatype=None):
        """
        Returns all columns of specified table in database
        tablename: String       Name of table
        dbname: String          Name of database    (Optional)
        datatype: Class         Class of datatype   (Optional)
        """
        try:
            if not dbname:
                dbname = self.__get_db_name()

            all_columns_query = _Config.get_all_columns_query.format(dbname=dbname, tablename=tablename)
            resultset = self.execute_query(all_columns_query, datatype=datatype)
            if resultset:
                return resultset
            else:
                raise Exception("No columns are retrieved.")
        except Exception, e:
            raise e

