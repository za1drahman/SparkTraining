from pyspark import SparkContext

class SparkHome:
    def __init__(self):
        self.sc= None

    def getSparkContext(self):
        self.sc= SparkContext()

    def getRDD(self, datatype, filename):
        if datatype == list:
            return self.sc.parallelize(filename)
        if datatype == str:
            return self.sc.textFile(filename)


obj1= SparkHome()
obj1.getSparkContext()
newRDD= obj1.getRDD(str, 'file:///home/training/titanic_data.csv')
newRDD1= obj1.getRDD(list, [5,4,3])
print newRDD.take(2)
print newRDD1.take(2)