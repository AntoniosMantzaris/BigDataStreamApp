package producer

import java.io.File
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import scala.io.Source
import models.RawGameState
import org.apache.kafka.common.serialization.StringSerializer

object GameProducer {
    def main(args: Array[String]) {
        val fileName = "../Data/test.csv"
        val topic = "leagueGames"
        val brokers = "localhost:9092"
        
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "GameProducer")
        val producer = new KafkaProducer[String, String](props, new StringSerializer(), new StringSerializer())
        
        val bufferedSource = Source.fromFile(fileName)

        for (line <- bufferedSource.getLines()) {
            val matchId = line.split(",")(0)
            val gameStateRecord = new ProducerRecord[String, String](topic, matchId, line)
            
            producer.send(gameStateRecord)
            println(gameStateRecord)
            Thread.sleep(100)
        }

        producer.close()
        bufferedSource.close()
    }
}