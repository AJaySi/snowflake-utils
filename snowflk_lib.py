"""
This module is a snowflake wrapper for instrumentation.
Author: ajay.singh
"""

import snowflake.connector
# This is optional, snowflake login details can also be passed directly.
from aws_lib import get_secret


# fixme: export TMPDIR=/large_tmp_volume
# Note that ACCOUNT might require the region and cloud platform where
# your account is located, in the form of
# '<your_account_name>.<region>.<cloud>' e.g. 'xy12345.us-east-1.aws'
# fixme: Get below params from params.json.

# Return snowflake connection object.
def conn_obj(conn='config'):
    """
    Common utils function to run probetest server rest verbs.
    Args:
      None
    Returns:
      Snowflake connection object
    Raises:
      Snowflake connection error
    """
    
    INFO("Connecting to Snowflake")
    # Integrating AWS secrets : 
    # Functionality to read secrets/passphrases etc from AWS secrets. 
	# Keeping them in config files is a security
    # problem. Using them from AWS secrets, gives us a secure way to use
    # setup secrets from common files. 
    url, user, account, warehouse, password, schema, role, database =\
        get_secret()
    
    try:
        con = snowflake.connector.connect(
            user=user,
            role=role,
            password=password,
            account=account,
            # Below, is needed in case of OKTA based auth.
            #authenticator='https://doamin-name.okta.com/app/snowflake/sso/saml',
            warehouse=warehouse,
            database=database
        )
        print("INFO: Successfully connected to snowflake.")
    except snowflake.connector.errors.DatabaseError as err:
        print("EXIT: Failed to connect to Snowflake DB: {}.".format(err))
        exit(1)
    
    snowflk = con.cursor()
    
    # Check also, if the connection was successful.
    try:
      ret = snowflk.execute("SELECT current_version()")
      one_row = ret.fetchone()
      INFO("Current Version: {}".format(one_row[0]))
    
      # Set current context for Database, Schema, and Warehouse
      INFO("INFO: Selecting Snowflake warehouse: {} and database: {}."\
          .format(warehouse, database))
      snowflk.execute("USE WAREHOUSE " + warehouse)
      snowflk.execute("USE DATABASE " + database)
    except snowflake.connector.errors.ProgrammingError as err:
      ERROR("EXIT: Snowflake Error {0} ({1}): {2} ({3})".\
            format(err.errno, err.sqlstate, err.msg, err.sfqid))
    
    # Return snowflake connection object.
    return snowflk


# Common function to select from the given snowflake table.
def select_from_table(table_name, nrows='all'):
    """
    Common utils function to select items from snowflake table.
    Args:
      table_name(string): Snowflake table name
      nrows(string): rows to select
    Returns:
      Query Result
    """
    results = ''
    # To get the specified number of rows at a time, use the
    # fetchmany method with the number of rows
    if 'all' in nrows:
        sel_query = "SELECT * FROM {}".format(table_name)
        results = exec_query(sel_query)
        for rec in results:
            INFO("Rows: {}, {}".format(rec[0], rec[1]))
    elif 'total' in nrows:
        INFO("Count Number of Rows in given table and views.")
        tot_query = "SELECT COUNT(*) from {}".format(table_name)
        results = exec_query(tot_query)
        for rec in results:
            INFO("Toal number of records {}".format(rec[0]))
    
    # Return the snowflake table selection result/output.
    return results


# This is an attempt at getting performance statistics from snowflake.
# How much time did the previous sql statement and other profiling.
def hist_profile():
  """
  Common function to get history and profiling data from snowflake
  Args:
    None
  Returns:
    Query Result
  """
  INFO("Query History and Profiling")
  ret = exec_query("select *\
     from\
     table(information_schema.query_history(dateadd('hours',-1,\
     current_timestamp()),current_timestamp())) order by start_time;")
  for rec in ret:
    INFO("Query History: {}".format(rec[0]))

  INFO("WAREHOUSE Query Profiling")
  ret = exec_query("select query_text,\
     warehouse_size, total_elapsed_time, bytes_scanned,\
     bytes_scanned, rows_produced, compilation_time, execution_time,\
     queued_provisioning_time, error_code, error_message,\
     start_time, end_time\
     from\
     table(information_schema.QUERY_HISTORY_BY_WAREHOUSE(warehouse))\
     order by start_time desc;")
  for rec in ret:
    INFO("Warehouse query profile {}".format(rec))


def drop_table(table_name):
  """
  Common function for cleanup of all. Useful to call at suite destroy
  TBD: Change the function to drop single named table.
  Args:
    table_name(string) : Snowflake table name
  Returns:
    None
  """
  INFO("\nDrop table")
  tab_drop = "DROP TABLE IF EXISTS {}".format(table_name)
  retop = exec_query(tab_drop)
  INFO("Drop table output: {}".format(retop))

  # fixme: Relying on probetest cleanup at suite level.
  #print("\nDrop Schema")
  #sch_drop = "DROP SCHEMA IF EXISTS testschema_mg"
  #exec_query(sch_drop)

  #print("\nDrop Database")
  #db_drop = "DROP DATABASE IF EXISTS testdb_mg"
  #exec_query(db_drop)

  #print("\nDrop Warehouse")
  #wh_drop = "DROP WAREHOUSE IF EXISTS tiny_warehouse_mg"
  #exec_query(wh_drop)


# Common function to execute query for the given snowflake connection.
# TBD: Code can be changed to pass the connection object with sql query 
# to execute with the same context.
def exec_query(query):
    """
    Common function to fire snowflake queries and do exception handling
    Args:
      query(string) : Snowflake Query to fire
    Returns:
      Query result
    Raises:
      Snowflake Connection Error
    """
    STEP("Connecting to snowflake")
    snowflk = conn_obj()

    # We should keep an eye of query execution time, for performance.
    timeout = 500
    try:
        INFO("Executing Snowflake Query: {}".format(query))
        ret_op = snowflk.execute(query, timeout=timeout).fetchall()
    except snowflake.connector.errors.ProgrammingError as err:
      if err.errno == 604:
        INFO("FAILED: Snowflake Query with timeout of {}".format(timeout))
        #snowflk.execute("rollback")
      else:
        INFO("Snowflake Error {0} ({1}): {2} ({3})".format(err.errno, err.sqlstate,\
            err.msg, err.sfqid))
        raise err
    
    # This, then mandates closing/connecting for every query.
    # Let the calling function take care of closing connection.
    snowflk.close()
    return ret_op
