
import pandas as pd
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
 
###Remove espaços do header<br/>
adclicksDataFrame = pd.read_csv('./ad-clicks.csv')<br/>
adclicksDataFrame = adclicksDataFrame.rename(columns=lambda x: x.strip())<br/>
adclicksDataFrame.head(n=2)<br/>

###Adiciona a coluna adCount<br/>
adclicksDataFrame['adCount'] = 1<br/>
adclicksDataFrame.head(n=2)<br/>

###Remove espaços do header<br/>
buyclicksDataFrame = pd.read_csv('./buy-clicks.csv')<br/>
buyclicksDataFrame = buyclicksDataFrame.rename(columns=lambda x: x.strip())<br/>
buyclicksDataFrame.head(n=2)<br/>

###Seleciona as colunas `userID` and `price` e remove as demais<br/>
PurchasesDataFrame = buyclicksDataFrame[['userId','price']]<br/>
PurchasesDataFrame.head(n=2)<br/>

###Seleciona as colunas `userID` e `adCount` e remove as demais<br/>
useradClicksDataFrame = adclicksDataFrame[['userId','adCount']]<br/>
useradClicksDataFrame.head(n=2)<br/>

###Cria novo arquivo agregando a coluna adCount pela  coluna userId<br/>
PerUserDataFrame = useradClicksDataFrame.groupby('userId').sum()<br/>
PerUserDataFrame = PerUserDataFrame.reset_index()<br/>

###Renomeia as colunas <br/>
PerUserDataFrame.columns = ['userId','totalAdClicks']<br/>
PerUserDataFrame.head(n=2)<br/>

###Cria novo arquivo agregando a coluna price pela coluna  userId<br/>
revenuePerUserDataFrame = PurchasesDataFrame.groupby('userId').sum()<br/>
revenuePerUserDataFrame = revenuePerUserDataFrame.reset_index()<br/>

###Renomeia as colunas <br/>
revenuePerUserDataFrame.columns = ['userId','revenue']<br/>
revenuePerUserDataFrame.head(n=2)<br/>

###Junta os dois arquivos (PerUserDataFrame + revenuePerUserDataFrame)<br/>
###userid, adCount, price<br/>
combinedDataFrame = PerUserDataFrame.merge(revenuePerUserDataFrame, on='userId')<br/>
combinedDataFrame.head(n=2)<br/>

###Cria o Dataset de treinamento (training)<br/>
###A coluna useriD será removida <br/>

trainingDataFrame = combinedDataFrame[['totalAdClicks','revenue']]<br/>
trainingDataFrame.head(n=2)<br/>

###Converte os Datasets em formato que pode ser entendido pelo Kmeans.train function<br/>
sqlContext = SQLContext(sc)<br/>
pdf = sqlContext.createDataFrame(trainingDataFrame)<br/>

###TotalAdClicks,revenue<br/>
parsedData = pdf.rdd.map(lambda line: array([line[0], line[1]]))<br/> 
my_kmmodel = KMeans.train(parsedData, 2, maxIterations=10, runs=10, initializationMode="random")<br/>

###Veja abaixo o cluster que iremos analizar<br/>
print(my_kmmodel.centers)<br/>