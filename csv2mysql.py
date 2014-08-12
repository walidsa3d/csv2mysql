#!/usr/bin/env python
 
import sys
import MySQLdb
import csv
 
def main(user, db, table, csvfile):
 
    try:
        conn = getconn(user, db,)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit (1)
 
    cursor = conn.cursor()
 
    loadcsv(cursor, table, csvfile)
 
    cursor.close()
    conn.close()
 
def getconn(user, db, passwd=""):
    conn = MySQLdb.connect(host = "localhost",
                           user = user,
                           passwd = "",
                           db = db)
    return conn
 
def nullify(L):
    """Convert empty strings in the given list to None."""
 
    # helper function
    def f(x):
        if(x == ""):
            return None
        else:
            return x
 
    return [f(x) for x in L]
 
def loadcsv(cursor, table, filename):
 
    """
    Open a csv file and load it into a sql table.
    Assumptions:
     - the first line in the file is a header
    """
 
    f = csv.reader(open(filename)) 
    header = f.next()
    numfields = len(header) 
    query = buildInsertCmd(table, numfields) 
    for line in f:
        vals = nullify(line)
        cursor.execute(query, vals)
 
    return
 
def buildInsertCmd(table, numfields):
 
    """
    Create a query string with the given table name and the right
    number of format placeholders.
 
    example:
    >>> buildInsertCmd("foo", 3)
    'insert into foo values (%s, %s, %s)' 
    """
    assert(numfields > 0)
    placeholders = (numfields-1) * "%s, " + "%s"
    query = ("insert into %s" % table) + (" values (%s)" % placeholders)
    return query
 
if __name__ == '__main__':
    # commandline execution
 
    args = sys.argv[1:]
    if(len(args) < 4):
        print "error: arguments: user db table csvfile"
        sys.exit(1)
 
    main(*args)