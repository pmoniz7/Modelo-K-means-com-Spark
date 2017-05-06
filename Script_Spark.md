
import pandas as pd
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
 
###Remove whitespaces from headers
adclicksDataFrame = pd.read_csv('./ad-clicks.csv')
adclicksDataFrame = adclicksDataFrame.rename(columns=lambda x: x.strip())
adclicksDataFrame.head(n=2)

###Add a colunm adCount
adclicksDataFrame['adCount'] = 1
adclicksDataFrame.head(n=2)

###Remove whitespaces from headers
buyclicksDataFrame = pd.read_csv('./buy-clicks.csv')
buyclicksDataFrame = buyclicksDataFrame.rename(columns=lambda x: x.strip())
buyclicksDataFrame.head(n=2)

###Select `userID` and `price` and drops all others columns
PurchasesDataFrame = buyclicksDataFrame[['userId','price']]
PurchasesDataFrame.head(n=2)

###Select `userID` and `adCount` and drops all others columns
useradClicksDataFrame = adclicksDataFrame[['userId','adCount']]
useradClicksDataFrame.head(n=2)

###Creates new file by adding each adCount per userId
PerUserDataFrame = useradClicksDataFrame.groupby('userId').sum()
PerUserDataFrame = PerUserDataFrame.reset_index()

###Rename the columns
PerUserDataFrame.columns = ['userId','totalAdClicks']
PerUserDataFrame.head(n=2)

###Creates new file by adding each price per userId
revenuePerUserDataFrame = PurchasesDataFrame.groupby('userId').sum()
revenuePerUserDataFrame = revenuePerUserDataFrame.reset_index()

###Rename the columns
revenuePerUserDataFrame.columns = ['userId','revenue']
revenuePerUserDataFrame.head(n=2)

###Join two files (PerUserDataFrame + revenuePerUserDataFrame)
###userid, adCount, price
combinedDataFrame = PerUserDataFrame.merge(revenuePerUserDataFrame, on='userId')
combinedDataFrame.head(n=2)

###Create training dataset
###The columns useris will be excluded

trainingDataFrame = combinedDataFrame[['totalAdClicks','revenue']]
trainingDataFrame.head(n=2)

###Convert the tables in a format that can be understood by the Kmeans.train function
sqlContext = SQLContext(sc)
pdf = sqlContext.createDataFrame(trainingDataFrame)

###TotalAdClicks,revenue
parsedData = pdf.rdd.map(lambda line: array([line[0], line[1]])) 
my_kmmodel = KMeans.train(parsedData, 2, maxIterations=10, runs=10, initializationMode="random")

###See below the clusters that we will analyze
print(my_kmmodel.centers)