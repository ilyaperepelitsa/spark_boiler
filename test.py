import findspark

import pyspark
import random
import os

spark_location='/usr/local/Cellar/apache-spark/2.4.4/libexec' # Set your own
java8_location= '/Library/Java/JavaVirtualMachines/jdk1.8.0_221.jdk/Contents/Home' # Set your own


os.environ['JAVA_HOME'] = java8_location
os.environ['PYSPARK_SUBMIT_ARGS'] = "--master spark://192.168.2.40:7077"

findspark.init(spark_home=spark_location)

# sc = pyspark.SparkContext(appName="Pi")
sc = pyspark.SparkContext()
# sc = pyspark.SparkConf()


num_samples = 100000000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1


count = sc.parallelize(range(0, num_samples)).filter(inside).count()

pi = 4 * count / num_samples
print(pi)

sc.stop()
