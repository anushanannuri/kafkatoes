package com.spark.scala.demo

import java.util.Date
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.kafka.common.serialization.StringDeserializer
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

object dataelastic {
  
  def main(args: Array[String]) {

    
    Logger.getLogger("org").setLevel(Level.ERROR);
    
    val brokers = "localhost:9092"
    val topics = "moviereview"
    
    val spark = SparkSession.builder().appName("dataelastic")
      .master("local[*]")
      .getOrCreate();
    
    val ssc = new StreamingContext(spark.sparkContext,Seconds(20))
    
    
    
    val kafkaParams = Map[String,String](
        "metadata.broker.list" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer].toString(),
        "value.deserializer" -> classOf[StringDeserializer].toString(),
        "auto.offset.reset" -> "largest")
        
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,kafkaParams,Set(topics))
        
    import spark.implicits._
    
    val lines = messages.map(_._2)
    
    lines.foreachRDD(
        rdd => {

          val line = rdd.toDS()
          
          println("Old Schema");
          line.show()
          val dateformat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
          val currentdate = dateformat.format(new Date)
          val modifiedDF = line.withColumn("_tmp", split(col("value"),",")).select(
              $"_tmp".getItem(0).as("userId").cast(sql.types.IntegerType),
              $"_tmp".getItem(1).as("movieId").cast(sql.types.IntegerType),
              $"_tmp".getItem(2).as("rating").cast(sql.types.DoubleType),
              $"_tmp".getItem(3).as("timestamp")).withColumn("Date", lit(currentdate))
              
          println("With new fields")
          modifiedDF.show()
          modifiedDF.write
          .format("org.elasticsearch.spark.sql")
          .option("es.port","9200")
          .option("es.nodes","localhost")
          .mode("append")
          .save("movieratingspark/doc")
         
        })
        
        
      ssc.start()
      ssc.awaitTermination()
  }
  
}