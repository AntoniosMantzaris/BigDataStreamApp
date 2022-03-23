package userinterface

import models.GamePrediction

import scala.io.StdIn.readLine
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnectorConf


import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector._
import org.apache.spark.SparkContext

import org.apache.spark.sql.functions.rand


object QueryConsole {

    val host = "127.0.0.1:9042"
    val clusterName = "Test Cluster"
    val keyspace = "dicproject"
    val tableName = "gamepredictions"
    
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("QueryConsole")
            .config("spark.master", "local")
            .getOrCreate()

        spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._
        
        val gamePredictions = spark
            .read
            .cassandraFormat(tableName, keyspace)
            .load()
            .as[GamePrediction]


        var input = getUserInput()

        while (input != "q") {
            if (input == "r") {
                gamePredictions.orderBy(rand()).limit(1).show()
            }
            
            else if (isNumeric(input)) {
                val matchId = input.toLong
                val gamePrediction = gamePredictions
                    .filter(_.matchid == matchId)
                
                if (gamePrediction.isEmpty) {
                    println("Invalid match id.")
                } else {
                    gamePrediction.show()
                }
            }

            else {
                println("Invalid input. Please try again.")
            }

            input = getUserInput()
        }


        val correctClassifications = gamePredictions
            .map(classificationScore)
            .reduce(_ + _)
        
        val accuracy = correctClassifications / gamePredictions.count().toDouble
        print("Accuracy on all entries is: ")
        println(accuracy)
        
        spark.close()
    }

    def classificationScore(prediction: GamePrediction): Int = {
        val prediction_of_blue_win = if (prediction.probability_prediction_blue_win > 0.5) 1 else 0
        if (prediction_of_blue_win == prediction.bluewin) 1 else 0
    }

    def isNumeric(inputString: String) =
        inputString.forall(_.isDigit)

    def getUserInput() = {
        println("Insert the match id for the game you want a prediction for, or press")
        println("[R] to display a random game.\n[Q] to exit this program")

        readLine().toLowerCase()
    }
}