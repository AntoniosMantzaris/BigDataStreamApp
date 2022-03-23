

package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class AverageState(sum: Int=0, count: Int=0){
     def add (added_value: Int) =
      AverageState(
        this.sum + added_value,
        this.count + 1)

    def average() : Double = sum.toDouble / count
}

object KafkaSpark {
  def main(args: Array[String]) {
    // make a connection to Kafka and read (key, value) pairs from it
    val conf = new SparkConf().setMaster("local[2]").setAppName("lab2task1")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")

    val topicSet = Set("avg")
    val kafkaConf = Map(
                        "metadata.broker.list" -> "localhost:9092",
                        "zookeeper.connect" -> "localhost:2181",
                        "group.id" -> "kafka-spark-streaming",
                        "zookeeper.connection.timeout.ms" -> "1000")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                    ssc, kafkaConf, topicSet)


    val lines = messages.map(x => x._2)  //key=null, value=string,string
    val comma = lines.map(x => x.split(","))   //string,string-->(string,string)
    val pairs = comma.map(x => (x(0), x(1).toInt))  //(string, Integer)



    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Int], state: State[AverageState]): (String, Double) ={
        val input = value.getOrElse(0)
        val old_st = state.getOption.getOrElse(AverageState())
        val new_st = old_st.add(input)
        state.update(new_st)
        (key, new_st.average())
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
