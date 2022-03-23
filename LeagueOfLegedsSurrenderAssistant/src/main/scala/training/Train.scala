package trainModel

import parseGame.GameParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.Dataset
import models.GameState
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressionModel

object Train {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Modeltraining")
            .config("spark.master", "local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        val dataframe = spark
            .read
            .option("inferSchema", true)
            .option("delimiter", ",")
            .option("header", true)
            .csv("../Data/train.csv")

        val dataset = GameParser.parseGameWithLabels(spark, dataframe)
        dataset.printSchema()
        train(dataset)
        spark.close()
    }

    def train(trainingData: Dataset[GameState]) = {
        val features = Array(
            "deltaAverageLevel",
            "deltaGold",
            "deltaMinionsKilled",
            "deltaJungleMinionsKilled",
            "blueTowersDestroyed",
            "redTowersDestroyed",
            "blueHeraldKills",
            "redHeraldKills",
            "blueDragonKills",
            "redDragonKills")
            
        val featureCols = Array()
        
        val assembler = new VectorAssembler()
            .setInputCols(features)
            .setOutputCol("features")

        // Train a RandomForest model.
        val rf = new RandomForestRegressor()
            .setLabelCol("blueWin")
            .setFeaturesCol("features")

        // Chain indexer and forest in a Pipeline.
        val pipeline = new Pipeline()
            .setStages(Array(assembler, rf))

        // Train model. This also runs the indexer.
        val model = pipeline.fit(trainingData)

        // Select (prediction, true label) and compute test error.
        val evaluator = new RegressionEvaluator()
            .setLabelCol("blueWin")
            .setPredictionCol("prediction")
            .setMetricName("mae")

        model
            .write
            .overwrite()
            .save("../SurrenderModel")
            
        println("Saved the model")
    }
}