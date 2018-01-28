#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def check_if_table_exists(table_name, openconnection):
    cur = openconnection.cursor()

    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]

    cur.execute('select count(*) from information_schema.tables where table_name=\'' + table_name + '\' and table_catalog=\'' + db_name + '\'')
    count = cur.fetchall()[0][0]

    cur.close()

    if count == 1:
        return True
    else:
        return False


def create_table(table_name, openconnection):
    create_command = ""
    if table_name == 'metadata':
        create_command = 'create table metadata (current_round_robin_fragment int, current_range_fragment int)'
    elif table_name == 'ratings':
        create_command = 'create table ratings (userid int, movieid int, rating real)'
    elif 'ratings' in table_name:
        create_command = 'create table ' + table_name + ' (userid int, movieid int, rating real)'
    else:
        print "UNKNOWN TABLE. ABORTING !!!"
        exit(2)

    cur = openconnection.cursor()
    cur.execute(create_command)
    openconnection.commit()
    cur.close()


def insert_ratings_record_to_table(userid, movieid, ratings, table_name, openconnection):
    cur = openconnection.cursor()
    cur.execute("insert into " + table_name + " values (" + userid + "," + movieid + "," + ratings + ")")
    openconnection.commit()
    cur.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    if not check_if_table_exists(ratingstablename, openconnection):
        create_table(ratingstablename, openconnection)

    if not check_if_table_exists('metadata', openconnection):
        create_table('metadata', openconnection)

    with open(ratingsfilepath, 'r') as input_file:
        file_data = input_file.read()
        for line in file_data.split("\n"):
            userid, movieid, rating, timestamp = line.split("::")
            insert_ratings_record_to_table(userid, movieid, rating, "ratings", openconnection)


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def deletepartitionsandexit(openconnection):
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()