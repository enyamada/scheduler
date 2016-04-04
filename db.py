
from time import strptime, strftime
from datetime import datetime

import logging
import logging.handlers
import os
import time


import MySQLdb




def update_db (db, job_id, **kwargs):

    cursor = db.cursor()

    set_clause = ""
    for k in kwargs:
        set_clause = set_clause + "%s='%s'," % (k, kwargs[k])

    set_clause = set_clause[:-1]

    sql = "UPDATE jobs SET %s WHERE id=%s" % (set_clause, job_id)
    logging.debug ("Update db: %s" % sql)
    cursor.execute (sql)



def job_db_data (db, job_id, *cols):

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute ("SELECT * FROM jobs WHERE id=%s" % job_id)
    row = cursor.fetchone()

    res = []
    for col in cols:
        res.append (row.get(col, None))

    return res


def open_connection(config):

   conn = MySQLdb.connect (config["host"], config["user"], config["secret"], config["db"])
   conn.autocommit(True)
   return conn



