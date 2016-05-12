import MySQLdb.cursors

### Database config ###
default_autocommit = False
default_cursor_datatype_for_columns = tuple
default_cursor_class = MySQLdb.cursors.Cursor
### Database Queries ###
get_all_columns_query = 'select COLUMN_NAME from information_schema.columns where table_schema = "{dbname}" and table_name = "{tablename}"'