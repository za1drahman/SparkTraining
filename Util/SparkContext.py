from pyspark import SparkContext

class Context:
    def __init__(self):
        self.sparkContext= None

    def getContext(self):
        self.sparkContext= SparkContext()
        return self.sparkContext

    def __del__(self):
        self.sparkContext.stop()



