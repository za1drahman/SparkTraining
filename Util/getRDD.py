from Util import SparkContext

class getRDD:

    def __init__(self):
        self.sparkobj= SparkContext.Context()
        self.sc = self.sparkobj.getContext()

    def getRDDfromCollection(self, data):
        return self.sc.parallelize(data)

    def getRDDfromFile(self,filename):
        return self.sc.textFile(filename)

