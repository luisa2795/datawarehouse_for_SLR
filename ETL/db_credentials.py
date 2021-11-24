from variables import datawarehouse_name

#sql server (target DWH db)
datawarehouse_db_config= {
    'Trusted_connection': 'yes',
    'driver': '{SQL Server}',
    'server': 'datawarehouse_sql_server',
    'database': '{}'.format(datawarehouse_name),
    'user': 'your_db_username',
    'password': 'your_db_password',
    'autocommit': True,
}

#source files are not in db, filepath specified in variables