
import pandas as pd
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
 
###Remove whitespaces from headers<br/>
adclicksDataFrame = pd.read_csv('./ad-clicks.csv')<br/>
adclicksDataFrame = adclicksDataFrame.rename(columns=lambda x: x.strip())<br/>
adclicksDataFrame.head(n=2)<br/>

###Add a colunm adCount<br/>
adclicksDataFrame['adCount'] = 1<br/>
adclicksDataFrame.head(n=2)<br/>

###Remove whitespaces from headers<br/>
buyclicksDataFrame = pd.read_csv('./buy-clicks.csv')<br/>
buyclicksDataFrame = buyclicksDataFrame.rename(columns=lambda x: x.strip())<br/>
buyclicksDataFrame.head(n=2)<br/>

###Select `userID` and `price` and drops all others columns<br/>
PurchasesDataFrame = buyclicksDataFrame[['userId','price']]<br/>
PurchasesDataFrame.head(n=2)<br/>

###Select `userID` and `adCount` and drops all others columns<br/>
useradClicksDataFrame = adclicksDataFrame[['userId','adCount']]<br/>
useradClicksDataFrame.head(n=2)<br/>

###Creates new file by adding each adCount per userId<br/>
PerUserDataFrame = useradClicksDataFrame.groupby('userId').sum()<br/>
PerUserDataFrame = PerUserDataFrame.reset_index()<br/>

###Rename the columns<br/>
PerUserDataFrame.columns = ['userId','totalAdClicks']<br/>
PerUserDataFrame.head(n=2)<br/>

###Creates new file by adding each price per userId<br/>
revenuePerUserDataFrame = PurchasesDataFrame.groupby('userId').sum()<br/>
revenuePerUserDataFrame = revenuePerUserDataFrame.reset_index()<br/>

###Rename the columns<br/>
revenuePerUserDataFrame.columns = ['userId','revenue']<br/>
revenuePerUserDataFrame.head(n=2)<br/>

###Join two files (PerUserDataFrame + revenuePerUserDataFrame)<br/>
###userid, adCount, price<br/>
combinedDataFrame = PerUserDataFrame.merge(revenuePerUserDataFrame, on='userId')<br/>
combinedDataFrame.head(n=2)<br/>

###Create training dataset<br/>
###The columns useris will be excluded<br/>

trainingDataFrame = combinedDataFrame[['totalAdClicks','revenue']]<br/>
trainingDataFrame.head(n=2)<br/>

###Convert the tables in a format that can be understood by the Kmeans.train function<br/>
sqlContext = SQLContext(sc)<br/>
pdf = sqlContext.createDataFrame(trainingDataFrame)<br/>

###TotalAdClicks,revenue<br/>
parsedData = pdf.rdd.map(lambda line: array([line[0], line[1]]))<br/> 
my_kmmodel = KMeans.train(parsedData, 2, maxIterations=10, runs=10, initializationMode="random")<br/>

###See below the clusters that we will analyze<br/>
print(my_kmmodel.centers)<br/>