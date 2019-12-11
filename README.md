# snowflake-utils
Snowflake python connector helper functions/utils

The main function is 'exec_query' which does the snowflake login and returns query result.

The snowflake login details are fetched from AWS secrets and the code for the same is in 'aws_lib.py'.
This is optional, if no AWS, then creating a local config file with encrypted snowflake parameters is another option.

As an example, connecting to snowflake, firing valid sql string/statement and return its output is present.
Other snowflake options like drop tables, etc is also present.
