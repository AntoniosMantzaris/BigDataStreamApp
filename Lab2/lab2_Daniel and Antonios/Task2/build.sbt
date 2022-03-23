name := "spark_kafka"

version := "1.0"

scalaVersion := "2.12.8"
val sparkversion = "3.1.2"
val spark = "org.apache.spark"

libraryDependencies ++= Seq(
    spark %% "spark-streaming" % sparkversion,
    spark %% "spark-sql" % sparkversion,
    spark %% "spark-streaming-kafka-0-10" % sparkversion,
    spark %% "spark-core" % sparkversion,

    spark %% "spark-sql-kafka-0-10" % sparkversion
)