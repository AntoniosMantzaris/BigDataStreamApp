package sparkstreaming

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

object StructuredStream {
    // def updateAverage(key: String, records: Iterator[Record], state: GroupState[AverageState]) : Average = {
    //     val averageState = state.getOption.getOrElse(new AverageState())
    //     records.foreach(record => averageState.update(record.value))
    //     state.update(averageState)

    //     Average(key, averageState.average())
    // }
    
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Structured")
            .config("spark.master", "local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._
        
        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "avg")
            .load()

        val df2 = df.select(
                split(col("value"),",").getItem(0).as("key"),
                split(col("value"),",").getItem(1).as("value").cast(IntegerType))

        val ds = df2.as[Record]
        val averages = ds.groupByKey(_.key).agg(typed.avg(_.value))

        val query = averages
            .writeStream
            .format("console")
            .outputMode("complete")
            .start()

        query.awaitTermination()
    }
}