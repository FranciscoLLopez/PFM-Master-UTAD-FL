package org.utad.flopez.streamingkmeans.kafka

import java.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.feature.{ HashingTF, StandardScaler }
import org.utad.flopez.kmeans.functions._
import org.utad.flopez.kmeans.filesOps.printToFile
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author flopez
 */
object skmeansKafka {
  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[4]").setAppName("skmeansKafka").set("spark.executor.memory", "1g").set("spark.streaming.receiver.maxRate", "10000")
    
    val ssc = new StreamingContext(conf, Seconds(1L))
    
    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%
    
    // Create direct kafka stream with brokers and topics.
    // Only one kafka for this example localhost:9092
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")
    val topics = args.toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    val rawData = kafkaStream.map(_._2)
    
    // Output folder to save files
    val outputFolder = "ds/skmeansKafka/"
    
    val data: DStream[Vector] = rawData.transform(rdd => toVector(rdd))
    
    val model = new StreamingKMeans()
      .setK(k)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    val predictions = model.predictOn(data)

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
      val predictString = rdd.map(p => p.toString).collect().mkString("\n")
      
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputFolder, dateString + "-model", modelString)
      printToFile(outputFolder, dateString + "-predictions", predictString)
    }

    ssc.start()
    ssc.awaitTermination()
  }
  
  def toVector(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = buildLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    val original = originalAndData.values
    original
  }

}
