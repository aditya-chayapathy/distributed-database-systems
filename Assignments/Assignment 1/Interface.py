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
    if table_name == 'ratings':
        create_command = 'create table if not exists ratings (userid int, movieid int, rating real)'
    elif 'ratings' in table_name:
        create_command = 'create table if not exists ' + table_name + ' (userid int, movieid int, rating real)'
    else:
        print "UNKNOWN TABLE. ABORTING !!!"
        exit(2)

    cur = openconnection.cursor()
    cur.execute(create_command)
    openconnection.commit()
    cur.close()


def insert_ratings_record_to_table(userid, movieid, ratings, table_name, openconnection):
    cur = openconnection.cursor()
    cur.execute("insert into " + table_name + " values (" + str(userid) + "," + str(movieid) + "," + str(ratings) + ")")
    openconnection.commit()
    cur.close()


def create_partitions(numberofpartitions, type, openconnection):
    for i in range(numberofpartitions):
        create_command = ""
        if type == "range":
            create_command = 'create table if not exists range_part' + str(i) + ' (userid int, movieid int, rating real)'
        elif type == "round robin":
            create_command = 'create table if not exists rrobin_part' + str(i) + ' (userid int, movieid int, rating real)'
        else:
            print "INCORRECT TYPE. ABORTING !!!"
            exit(2)
        cur = openconnection.cursor()
        cur.execute(create_command)
        openconnection.commit()
        cur.close()


def get_partition_number_for_rating(interval_range, rating):
    partition = 0
    start_range = 0
    end_range = interval_range
    while True:
        if partition == 0:
            if rating >= start_range and rating <= end_range:
                return 0
            else:
                start_range = end_range
                end_range += interval_range
                partition += 1
        else:
            if rating > start_range and rating <= end_range:
                return partition
            else:
                start_range = end_range
                end_range += interval_range
                partition += 1


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    if not check_if_table_exists(ratingstablename, openconnection):
        create_table(ratingstablename, openconnection)

    with open(ratingsfilepath, 'r') as input_file:
        file_data = input_file.read()
        for line in file_data.split("\n"):
            userid, movieid, rating, timestamp = line.split("::")
            insert_ratings_record_to_table(userid, movieid, rating, "ratings", openconnection)


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if not check_if_table_exists(ratingstablename, openconnection):
        create_table(ratingstablename, openconnection)

    interval_range = 5 / float(numberofpartitions)
    create_partitions(numberofpartitions, "range", openconnection)

    cur = openconnection.cursor()
    cur.execute('select * from ratings')
    data = cur.fetchall()
    cur.close()
    for row in data:
        userid, movieid, rating = row
        partition_number = get_partition_number_for_rating(interval_range, rating)
        table_name = "range_part" + str(partition_number)
        insert_ratings_record_to_table(userid, movieid, rating, table_name, openconnection)


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if not check_if_table_exists(ratingstablename, openconnection):
        create_table(ratingstablename, openconnection)

    create_partitions(numberofpartitions, "round robin", openconnection)

    cur = openconnection.cursor()
    cur.execute('select * from ' + ratingstablename)
    data = cur.fetchall()
    cur.close()
    partition_number = 0
    for row in data:
        userid, movieid, rating = row
        table_name = "rrobin_part" + str(partition_number)
        insert_ratings_record_to_table(userid, movieid, rating, table_name, openconnection)
        partition_number += 1
        if partition_number == numberofpartitions:
            partition_number = 0


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]
    cur.execute('select count(*) from information_schema.tables where table_name like \'%robin_part%\' and table_catalog=\'' + db_name + '\'')
    partitions_count = cur.fetchall()[0][0]

    partition_records_dict = {}
    for i in range(partitions_count):
        cur.execute('select count(*) from rrobin_part' + str(i))
        count = cur.fetchall()[0][0]
        partition_records_dict['rrobin_part' + str(i)] = count
    cur.close()

    partition_to_insert = 0
    if len(set(partition_records_dict.values())) != 1:
        for i in range(1, partitions_count):
            if partition_records_dict['rrobin_part' + str(i)] == partition_records_dict['rrobin_part' + str(i - 1)] - 1:
                partition_to_insert = i
                break

    table_name = "rrobin_part" + str(partition_to_insert)
    insert_ratings_record_to_table(userid, itemid, rating, table_name, openconnection)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]
    cur.execute('select count(*) from information_schema.tables where table_name like \'%range_part%\' and table_catalog=\'' + db_name + '\'')
    partitions_count = cur.fetchall()[0][0]

    interval_range = 5 / float(partitions_count)
    partition_number = get_partition_number_for_rating(interval_range, rating)
    table_name = "range_part" + str(partition_number)
    insert_ratings_record_to_table(userid, itemid, rating, table_name, openconnection)


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]
    cur.execute('drop table ratings')

    cur.execute('select table_name from information_schema.tables where table_name like \'%range_part%\' and table_catalog=\'' + db_name + '\'')
    range_partitions = cur.fetchall()
    for table in range_partitions:
        table_name = table[0]
        cur.execute('drop table ' + table_name)

    cur.execute('select table_name from information_schema.tables where table_name like \'%rrobin_part%\' and table_catalog=\'' + db_name + '\'')
    round_robin_partitions = cur.fetchall()
    for table in round_robin_partitions:
        table_name = table[0]
        cur.execute('drop table ' + table_name)

    openconnection.commit()
    cur.close()


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