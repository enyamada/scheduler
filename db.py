"""

Module containing some functions that handle the database.

"""

from time import strptime, strftime
from datetime import datetime

import os
import time

import logging
import logging.handlers

import MySQLdb





def update_db (db, job_id, **kwargs):
    """ 

    Updates the information stored in the database (jobs table) regarding
     a specific job.

    Args:
        db: A database connection (as returned by the open_connection function)
        job_id: The job id
        **kwargs: Here you can specify the columns/values to be updated.

    Example:
         update_db(db_conn, 10, callback="http://my-callback.com") updates the
             database (using the db_conn connection) regarding the job whose 
             id is 10 setting the callback column to ="http://my-callback.com"

    """

    cursor = db.cursor()

    set_clause = ""
    for k in kwargs:
        set_clause = set_clause + "%s='%s'," % (k, kwargs[k])

    set_clause = set_clause[:-1]

    sql = "UPDATE jobs SET %s WHERE id=%s" % (set_clause, job_id)
    logging.debug("Update db: %s" % sql)
    cursor.execute(sql)



def job_db_data (db, job_id, *cols):
    """

    Reads information stored in the database about a specific job and
    returns as a list.

    Args:
        db: A database connection (as returned by the open_connection function)
        job_id: The job id
        *cols: List of columns to be returned
   
    Returns:
        A list of columns (as specified by the arguments)

    Example:
         job_db_data (db_conn, 10, "status", "req_state") reads the
             database (using the db_conn connection) regarding the job whose
             id is 10 and returns as a list the values found in the "status"
             and "req_state" columns.

    """


    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT * FROM jobs WHERE id=%s" % job_id)
    row = cursor.fetchone()

    res = []
    for col in cols:
        res.append(row.get(col, None))

    return res


def open_connection(config):
    """

    Opens a connection to the database as specified by the config argument.

    Args:
        config: A dictionary containing the database connection parameters.
            Should contain the host, user, secret and db keys specifying
            the host where the database server is, the user/password to 
            be used and the database name to log into.

    Returns:
        A db connection (to be used on other db calls)


    """


    conn = MySQLdb.connect(config["host"], config["user"], config["secret"], config["db"])
    conn.autocommit(True)
    return conn



