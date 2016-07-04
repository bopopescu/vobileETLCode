import csv

with open('../data/noinfring','r')as f:
#       csv_file=csv.DictReader(f)      print csv_file.fieldnames
    reader = csv.reader(f, delimiter='	')
    for line in f:
        print line, len(line)

        break
