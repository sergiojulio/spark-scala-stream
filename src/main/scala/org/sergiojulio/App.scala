package org.sergiojulio
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, TimestampType, StringType, FloatType}
import org.apache.spark.sql.functions.{udf, col, from_json}

object App {

  def main(args: Array[String]): Unit = {

    print("\n\n>>>>> START OF PROGRAM <<<<<\n\n")

    val spark = SparkSession.builder().master("local[4]").appName("spark-scala-stream").getOrCreate()

    val streamDF = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "localhost:9092")
                      .option("subscribe", "kafka_topic")
                      .load()

    print("Printing Schema:")

    streamDF.printSchema()

    val schema = new StructType()
      .add("created", TimestampType)
      .add("text", StringType)

    val processedStreamDF = streamDF.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    processedStreamDF.writeStream
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
      .option("truncate", "true")
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()

    print("Stream Processing Successfully Completed")

    print("\n\n>>>>> END OF PROGRAM <<<<<\n\n")

    spark.close()

  }
}
