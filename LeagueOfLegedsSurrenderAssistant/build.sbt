name := "LOLSurrender"

version := "1.0"

scalaVersion := "2.12.15"
val sparkversion = "3.1.2"
val spark = "org.apache.spark"

libraryDependencies ++= Seq(
    spark %% "spark-sql" % sparkversion,
    spark %% "spark-core" % sparkversion,
    spark %% "spark-mllib" % sparkversion,
    spark %% "spark-sql-kafka-0-10" % sparkversion,

    "org.apache.kafka" %% "kafka" % "3.0.0",

    "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0",
    "joda-time" % "joda-time" % "2.10.12"
)