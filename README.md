# btcloadprices

Load BTC Historical Prices and Predicted Prices

## Getting Started

These instructions will get you a copy of the project up and running to verify the data load jobs.

### Prerequisites
Install sbt from [here](https://www.scala-sbt.org/1.0/docs/Setup.html)

Install MongoDB instance (if you don't have one for this code to run, click [here](https://docs.mongodb.com/manual/administration/install-community/) to install it.

### Setup
Clone this repository.
```
git clone https://github.com/ashokdamacharla/btcloadprices.git
```
Import any editor or open any command prompt, move to project folder where sbt.* files are present.


### Run Data load Jobs

Run the Historical Prices Job.
```
sbt "runMain com.bitcoin.load.LoadPrices mongodb://localhost:27017/bcp.prices2 mongodb://127.0.0.1/bcp.prices2"
```

Run the Predict Prices Job
```
sbt "runMain com.bitcoin.predict.PredictPrice mongodb://localhost:27017/ mongodb://127.0.0.1/ bcp.prices2 bcp.predictions2"
```


