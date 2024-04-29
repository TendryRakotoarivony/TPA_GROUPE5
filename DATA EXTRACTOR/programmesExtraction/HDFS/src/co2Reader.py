# hadoop fs -put CO2.csv input

# pyspark --master "local[2]"

from pyspark import SparkContext

sc = SparkContext("local[2]", "TransformationCO2")

# Function used to replace missing values and problematic values
def intoInt(x):
    if x == '-':
        return x
    else:
        return int(x)

def valToMean(x, mean):
    if x == '-':
        return mean
    else:
        return x

# Load the data
co2 = sc.textFile('input/CO2.csv')

# 1. Replace the problematic characters
rdd0 = co2.map(lambda x: x.replace('Ã\xa0 aimant permanent,', 'à aimant permanent'))
rdd1 = rdd0.map(lambda x: x.split(","))
rdd2 = rdd1.map(lambda x: [elem.replace('\xa0', '') for elem in x])
rdd3 = rdd2.map(lambda x: [elem.replace('Ã©', 'é') for elem in x])
rdd4 = rdd3.map(lambda x: [elem.replace('€1', '€') for elem in x])
rdd5 = rdd4.map(lambda x: [elem.replace('€', '') for elem in x])
rdd6 = rdd5.map(lambda x: [elem.replace('Ã¨', 'è') for elem in x])

# 2. Fixing the problem of the first word of x[1] being split (MARQUES)
rdd7 = rdd6.map(lambda x: [x[0], x[1].split(' ')[0], x[2], x[3], x[4]])
# If the first word of x[1] is 'Land', then replace x[1] with 'Land Rover'
rdd8 = rdd7.map(lambda x: [x[0], x[1] if x[1] != 'LAND' else 'LAND ROVER', x[2], x[3], x[4]])
# if the first word of x[1] is '"KIA"', then replace x[1] with 'KIA'
rdd9 = rdd8.map(lambda x: [x[0], x[1] if x[1] != '"KIA"' else 'KIA', x[2], x[3], x[4]])
rdd9a = rdd9.map(lambda x: [x[0], x[1] if x[1] != '"KIA' else 'KIA', x[2], x[3], x[4]])
rdd9b = rdd9a.map(lambda x: [x[0], x[1] if x[1] != '"VOLKSWAGEN' else 'VOLKSWAGEN', x[2], x[3], x[4]])
rdd10 = rdd9b.filter(lambda x: x[0] != '')

# For each x[1] (Marques) with the same value, replace x[2], x[3] and x[4] with the mean of the column
rdd11 = rdd10.map(lambda x: (x[1], x[2], x[3], x[4]))
rdd12 = rdd11.map(lambda x: (x[0], intoInt(x[1]), intoInt(x[2]), int(x[3])))
# Fix the missing values
rddmean = rdd12.map(lambda x: x[1]).filter(lambda x: x != '-')
mean = rddmean.mean()
rdd13 = rdd12.map(lambda x: (x[0], valToMean(x[1], mean), x[2], x[3]))

# Reduce the rows with the same x[1] (Marques) and compute the mean of the columns
rdd14 = rdd13.map(lambda x: (x[0], (x[1], x[2], x[3], 1)))
rdd15 = rdd14.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2], x[3] + y[3]))
res = rdd15.map(lambda x: (x[0], x[1][0] / x[1][3], x[1][1] / x[1][3], x[1][2] / x[1][3]))

# 3. Transform the rows into strings
resInString = res.map(lambda x: x[0] + ',' + str(x[1]) + ',' + str(x[2]) + ',' + str(x[3]))

# 4. Save the result in multiple files
resInString.saveAsTextFile('output/TransformationCO2')
