#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math


def get_distance_between_points(point2_lat, point2_long, point1_lat, point1_long):
    r = 3959
    phi1 = math.radians(point1_lat)
    phi2 = math.radians(point2_lat)
    delta_phi = math.radians(point2_lat - point1_lat)
    delta_lambda = math.radians(point2_long - point1_long)
    a = math.sin(delta_phi / 2) * math.sin(delta_phi / 2) + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) * math.sin(delta_lambda / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = r * c

    return distance


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    result = collection.find({"city": cityToSearch})
    data = []
    for rec in result:
        data.append(rec["name"] + "$" + rec["full_address"] + "$" + rec["city"] + "$" + rec["state"])

    fh = open(saveLocation1, "w")
    fh.write("\n".join(data))
    fh.close()


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    result = collection.find({"categories": {"$all": categoriesToSearch}})
    data = []
    for rec in result:
        dist = get_distance_between_points(float(myLocation[0]), float(myLocation[1]), float(rec["latitude"]), float(rec["longitude"]))
        if dist <= maxDistance:
            data.append(rec["name"])

    fh = open(saveLocation2, "w")
    fh.write("\n".join(data))
    fh.close()

