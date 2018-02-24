#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import threading
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################

def checkAndCreateTable(InputTable, OutputTable, openconnection):
    cur = openconnection.cursor()

    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]

    cur.execute('select table_name from information_schema.tables where table_name =  \'' + OutputTable + '\' and table_catalog=\'' + db_name + '\'')
    res = cur.fetchall()
    if len(res) > 0:
        cur.close()
        return

    cur.execute('select column_name, udt_name from information_schema.columns where table_name =  \'' + InputTable + '\' and table_catalog=\'' + db_name + '\'')
    input_schema = cur.fetchall()

    column_spec = []
    for column in input_schema:
        (column_name, data_type) = column
        column_spec.append(str(column_name) + " " + str(data_type))
    create_output_table_statement = "create table " + str(OutputTable) + "(" + ",".join(column_spec) + ")"

    cur.execute(create_output_table_statement)
    cur.close()
    openconnection.commit()


def parallelPartitionSort(partitionNumber, inputTable, start, end, sortingColumnName, openconnection):
    table_name = "ParallelSortPartition" + str(partitionNumber)
    checkAndCreateTable(inputTable, table_name, openconnection)

    cur = openconnection.cursor()
    if partitionNumber != 4:
        cur.execute('select * from ' + inputTable + ' where ' + sortingColumnName + " >= " + str(start) + " and " + sortingColumnName + " < " + str(end) + " order by " + sortingColumnName + " asc")
    else:
        cur.execute('select * from ' + inputTable + ' where ' + sortingColumnName + " >= " + str(start) + " and " + sortingColumnName + " <= " + str(end) + " order by " + sortingColumnName + " asc")
    res = cur.fetchall()
    for record in res:
        cur.execute("insert into " + str(table_name) + " values %s", (record,))

    cur.close()
    openconnection.commit()


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    checkAndCreateTable(InputTable, OutputTable, openconnection)

    cur = openconnection.cursor()
    cur.execute('select min(' + SortingColumnName + '), max(' + SortingColumnName + ') from ' + InputTable)
    range_res = cur.fetchall()
    min_val = range_res[0][0]
    max_val = range_res[0][1]
    interval = (max_val - min_val) / float(5)

    threads = []
    start = min_val
    end = start + interval
    for i in range(5):
        t = threading.Thread(target=parallelPartitionSort, args=(i, InputTable, start, end, SortingColumnName, openconnection))
        threads.append(t)
        t.start()
        start = end
        end += interval

    for thread in threads:
        thread.join()

    for i in range(5):
        cur.execute('select * from ParallelSortPartition' + str(i) + ' order by ' + SortingColumnName + ' asc')
        partition_res = cur.fetchall()
        for record in partition_res:
            cur.execute("insert into " + str(OutputTable) + " values %s", (record,))
        cur.execute("drop table ParallelSortPartition" + str(i))

    cur.close()
    openconnection.commit()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
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
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
    	# Creating Database ddsassignment3
    	print "Creating Database named as ddsassignment3"
    	createDB();

    	# Getting connection to the database
    	print "Getting connection from the ddsassignment3 database"
    	con = getOpenConnection();

    	# Calling ParallelSort
    	print "Performing Parallel Sort"
    	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

    	# Calling ParallelJoin
    	print "Performing Parallel Join"
    	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

    	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
    	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
    	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

    	# Deleting parallelSortOutputTable and parallelJoinOutputTable
    	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
