from pyspark.sql import SparkSessionp
import csv

readPath = ''
writePath = ''

# Read from HDFS
df_load = sparkSession.read.csv(readPath)
df_load.show()

data = readCSV(df_load)


df = sparkSession.createDataFrame(data)

# Write into HDFS
df.write.csv(writePath)

def readCSV(fl):
    reader = csv.DictReader(open(fl, 'rb'))
    dict_list = []
    for line in reader:
        dict_list.append(line)
    return dict_listp

def sorted(data):
    return multikeysort(data, ['cca2', 'timestamp'])
    
def multikeysort(items, columns):
    from operator import itemgetter
    comparers = [((itemgetter(col[1:].strip()), -1) if col.startswith('-') else
                  (itemgetter(col.strip()), 1)) for col in columns]
    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, cmp=comparer)
