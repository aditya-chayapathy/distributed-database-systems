#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    fh = open("RangeQueryOut.txt", "w")

    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]

    cur.execute('select table_name from information_schema.tables where table_name like \'%rangeratingspart%\' and table_catalog=\'' + db_name + '\'')
    range_partitions = cur.fetchall()
    for table in range_partitions:
        table_name = table[0]
        cur.execute('select userid, movieid, rating from ' + str(table_name) + ' where rating >= ' + str(ratingMinValue) + ' and rating <= ' + str(ratingMaxValue))
        matches = cur.fetchall()
        for match in matches:
            partition = ""
            if "range" in table_name:
                partition = "RangeRatingsPart" + table_name[-1]
            else:
                partition = "RoundRobinRatingsPart" + table_name[-1]
            fh.write(str(partition) + "," + str(match[0]) + "," + str(match[1]) + "," + str(match[2]) + "\n")

    cur.execute('select table_name from information_schema.tables where table_name like \'%roundrobinratingspart%\' and table_catalog=\'' + db_name + '\'')
    round_robin_partitions = cur.fetchall()
    for table in round_robin_partitions:
        table_name = table[0]
        cur.execute('select userid, movieid, rating from ' + str(table_name) + ' where rating >= ' + str(
            ratingMinValue) + ' and rating <= ' + str(ratingMaxValue))
        matches = cur.fetchall()
        for match in matches:
            partition = ""
            if "range" in table_name:
                partition = "RangeRatingsPart" + table_name[-1]
            else:
                partition = "RoundRobinRatingsPart" + table_name[-1]
            fh.write(str(partition) + "," + str(match[0]) + "," + str(match[1]) + "," + str(match[2]) + "\n")

    cur.close()
    fh.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    fh = open("PointQueryOut.txt", "w")

    cur = openconnection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchall()[0][0]

    cur.execute('select table_name from information_schema.tables where table_name like \'%rangeratingspart%\' and table_catalog=\'' + db_name + '\'')
    range_partitions = cur.fetchall()
    for table in range_partitions:
        table_name = table[0]
        cur.execute('select userid, movieid, rating from ' + str(table_name) + ' where rating = ' + str(ratingValue))
        matches = cur.fetchall()
        for match in matches:
            partition = ""
            if "range" in table_name:
                partition = "RangeRatingsPart" + table_name[-1]
            else:
                partition = "RoundRobinRatingsPart" + table_name[-1]
            fh.write(str(partition) + "," + str(match[0]) + "," + str(match[1]) + "," + str(match[2]) + "\n")

    cur.execute(
        'select table_name from information_schema.tables where table_name like \'%roundrobinratingspart%\' and table_catalog=\'' + db_name + '\'')
    round_robin_partitions = cur.fetchall()
    for table in round_robin_partitions:
        table_name = table[0]
        cur.execute('select userid, movieid, rating from ' + str(table_name) + ' where rating = ' + str(ratingValue))
        matches = cur.fetchall()
        for match in matches:
            partition = ""
            if "range" in table_name:
                partition = "RangeRatingsPart" + table_name[-1]
            else:
                partition = "RoundRobinRatingsPart" + table_name[-1]
            fh.write(str(partition) + "," + str(match[0]) + "," + str(match[1]) + "," + str(match[2]) + "\n")

    cur.close()
    fh.close()
