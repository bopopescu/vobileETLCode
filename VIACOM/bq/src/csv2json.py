#!/usr/bin/env python
#coding:utf8

"""
Description: csv file convert to json
Author: 
Date: 2016-05-14

"""

import csv
import json
import sys

def getFieldNames(file_name, field_delimiter = "\t"):
    with open(file_name, "r") as f:
        for line in f:
            fieldnames = line.rstrip().split(field_delimiter)
            break

    return fieldnames

def csvToJson(csv_file, json_file, field_delimiter = "\t"):
    field_names = getFieldNames(file_name=csv_file, field_delimiter=field_delimiter)

    fcsv = open(csv_file, 'rbU')
    fjson = open(json_file, 'w')
    reader = csv.DictReader(fcsv, field_names,delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\r")
    #reader = csv.DictReader(fcsv, field_names,delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar = '"')
    i = 0
    for row in reader:
        i += 1
        if 1 == i:
            continue
        json.dump(row, fjson)
        fjson.write('\n')

    fjson.close()
    fcsv.close()

def main():
    if len(sys.argv) != 4:
        print "please input: csv_file json_file field_delimiter"
        sys.exit(0)
    csv_file, json_file, field_delimiter = sys.argv[1], sys.argv[2], sys.argv[3]
    csvToJson(csv_file, json_file, field_delimiter)

if __name__ == "__main__":
    main()
