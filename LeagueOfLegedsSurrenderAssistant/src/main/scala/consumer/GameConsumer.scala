package consumer

import models.{RawGameState, GamePrediction}

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split

import org.apache.spark.sql.expressions.scalalang.typed
import parseGame.GameParser
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.PipelineModel


import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector._
import org.apache.spark.SparkContext


object StructuredStream {

    val host = "127.0.0.1:9042"
    val clusterName = "Test Cluster"
    val keyspace = "dicproject"
    val tableName = "gamepredictions"

    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Structured")
            .config("spark.master", "local")
            .getOrCreate()

        spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val model = PipelineModel.read.load("../SurrenderModel")

        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "leagueGames")
            .load()
        
        val ds = df
            .select(col("value").cast(StringType))
            .map(row => row.getAs[String]("value"))
            .map(decodeLineToGameState)
        
        val gameStates = GameParser
            .parseGameWithLabels(spark, ds.toDF())

        val predictions = model
            .transform(gameStates)
            .select("matchId", "blueWin", "prediction")
            .withColumnRenamed("prediction", "probability_prediction_blue_win")
            .as[GamePrediction]
            .map(_.toCassandraRow())

        val query = predictions
            .writeStream
            .cassandraFormat(tableName, keyspace)
            .option("cluster", clusterName)
            .option("checkpointLocation", "../Checkpoints")
			.outputMode(OutputMode.Append())
			.start()

        query.awaitTermination()
    }

    def decodeLineToGameState(line: String) = {
        val attributes = line.split(",")

        RawGameState(
            attributes(0).toLong,
            attributes(1).toInt,
            attributes(2).toInt,
            attributes(3).toInt,
            attributes(4).toInt,
            attributes(5).toDouble,
            attributes(6).toInt,
            attributes(7).toInt,
            attributes(8).toInt,
            attributes(9).toDouble,
            attributes(10).toInt,
            attributes(11).toInt,
            attributes(12).toInt,
            attributes(13).toInt,
            attributes(14).toInt,
            attributes(15).toInt,
            attributes(16).toInt,
            attributes(17).toInt)
    }
}