# On Line Inferencing in Kafka Streams

This Kafka Streams application demonstrates using embedded ML library [Apache Mahout](https://github.com/apache/mahout) to perform
OnlineLogisticRegression of flight data from the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)

Specifically this application aims to do two things:

1. Demonstrate the ability to perform online inferencing by joining a KStream with a 
GlobalKTable (airport id is the key) containing coefficients/model that can be used to predict the
if a flight will arrive on time or not, by making an prediction with the flight data in the 
record.

2. Update the model by a separate stream (Processor API) that collects flight data and when
enough data is collected retrain a model and publish the updated coefficients to the Kafka topic
backing the GlobalKTable, ensuring up to date predictions and keeping the model up to date
in a streaming manner and hopefully improve our   

Initially we'll observe a poor prediction rate, around 50%, basically a coin flip.  But as we collect more data we are able
to build a better model and publish the new updated model to the GlobalKTable, resulting in much better prediction rates 
somewhere between 80-90%.  

Again the point of this application is not about machine learning algorithms per-se or how to build better machine-learning
models, but that we can leverage the GlobalKTable to publish and updated model/coefficients and improve our on-line
inferencing in _steaming_ manner without having to do a batch job.


This project uses Gradle and after cloning/downloading it is recommended to first run the `gradle` command.

It is assumed that a Kafka instance already installed and running.

To run this application

1. Create the following topics: `onlineRegression-by-airport`, `raw-airline-data`, `ml-data-input`, `predictions` .
2. To build the initial model and populate the GlobalKTable run `./gradlew popluateGlobalKTable` from  terminal window.
3. Then start the `KStreamsOnLinePredictions` application with `./gradlew runOnlinePredictions`
3. From a separate terminal window start the data feed of flight data that the application will make predictions for and additionally will 
be used to create new models and update the GlobalKTable: run `./gradlew runDataFeed`

