import csv
import json
import sys


def getFieldNames(file_name, field_delimiter = "\t"):
    with open(file_name, "r") as f:
        for line in f:
            fieldnames = line.rstrip().split(field_delimiter)
            break

    return fieldnames


filename = "../data/b"
targetfile = "../data/file.json"

def csvToJson(csv_file, json_file, field_delimiter = "\t"):
    field_names = getFieldNames(file_name=csv_file, field_delimiter=field_delimiter)

    fcsv = open(csv_file, 'r')
    fjson = open(json_file, 'w')
    reader = csv.DictReader(fcsv, field_names)
    i = 0
    for row in reader:
        i += 1
        print row
        json.dump(row, fjson)
        fjson.write('\n')

    fjson.close()
    fcsv.close()

def csvTojson1(csv_file, json_file):
    fjson = open(json_file, "w")

    i = 1
    with open(csv_file, "r") as f:
        for line in f.read():
            i += 1
            if 1 == i:
                continue

            row = line.strip().split("\t")
            print row
            json.dump(row, fjson)
            fjson.write('\n')


    fjson.close()


def test():
    if len(sys.argv) != 4:
        print "csv_file json_file field_delimiter"
        sys.exit(0)
    csv_file, json_file, field_delimiter = sys.argv[1], sys.argv[2], sys.argv[3]
    csvToJson(csv_file, json_file, field_delimiter)

def test1():
    csv_file = "../data/noinfring"
    json_file = "../data/file.json"
    csvTojson1(csv_file, json_file)

if __name__ == "__main__":
    test1()
