package org.utad.flopez.streamingkmeans.kafka.lightning

import java.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming.{ Seconds, StreamingContext, Duration }
import org.apache.spark.streaming.dstream._
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.feature.{ HashingTF, StandardScaler }
import org.utad.flopez.kmeans.functions._
import org.utad.flopez.kmeans.filesOps.printToFile
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.viz.lightning.Lightning
import scala.util.{ Random, Try }

/**
 * @author flopez
 */
object skmeansKafkaLSample {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").
      setAppName("skmeansKafkaLSample").set("spark.executor.memory", "1g").
      set("spark.streaming.receiver.maxRate", "10000")

    val ssc = new StreamingContext(conf, Duration(10))
    val k = 4 // 4 Calculo de Kmeans por R
    val valPCA = 5 // Calculo de variables PCA con el 91% acumulado 
    // Create direct kafka stream with brokers and topics.
    // Only one kafka for this example localhost:9092
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")
    val topics = args.toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    val rawData = kafkaStream.map(_._2)
    
    rawData.foreachRDD{ rdd =>
      println("Lenght each rdd with 1s: " + rdd.count())
    }
    
    
    val outputFolder = "ds/mySKMSkafkaSample/"

    val lgn = Lightning(host = "http://mytest103.herokuapp.com")
    lgn.createSession("skmeansKafkaSample")
    val label = Array(1, 2, 3, 4)
    val scatter = lgn.scatterstreaming(x = Array(-4.0, 2.0, 3.0, 20.0), y = Array(-5.0, 2.0, 3.0, 4.0), size = Array(4.3, 4.3, 4.3, 4.3), label = label)
    //val scatter2 = lgn.scatter(x, y, label, size, alpha, xaxis, yaxis)
    print("test>>>>>>>>>>>>>>>>    " + scatter.toString())

    val data: DStream[Vector] = rawData.transform(rdd => toVector2(rdd))

    val model = new StreamingKMeans()
      .setK(k)
      .setHalfLife(10, "batches")
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    val predictions = model.predictOn(data)

    predictions.foreachRDD { rdd =>

      val centers = model.latestModel().clusterCenters
      val modelString = centers
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
      val modelx = centers.map(_.apply(0))
      val modely = centers.map(_.apply(1))
      val modelx1 = Array(1.0, 3.4, 3.4, 5.3)
      //  val predictString = rdd.map(p => p.toString()).collect.mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      val pred = rdd.toArray()
      val c1 = Array(23, 42, 22)
      val c2 = Array(123, 142, 122)
      val c3 = Array(50, 60, 170)
      val c4 = Array(140, 140, 140)
      val size = Array(4.3, 4.3, 4.3, 4.3)

      val color = Array(c1, c2, c3, c4)
      printToFile(outputFolder, dateString + "-model", modelString)
      printToFile(outputFolder, dateString + "-modelx", modelx.deep.toString())
      printToFile(outputFolder, dateString + "-modely", modely.deep.toString())

      if (Try(scatter.append(modelx, modely, color, label, size)).isFailure) { println("NOT WORK!!!") } //(modelx, modely, color=black)
      println("Y: " + modely.deep.toString)

    }

    ssc.start()
    ssc.awaitTermination()
  }
  def toVector(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = buildLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    val original = originalAndData.values
    if (!original.isEmpty()) {
      val scaledData = new StandardScaler(false, true).fit(original).transform(original)
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      scaledData
    } else {
      original
    }

  }

  def toVector2(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = buildCategoricalAndLabelFunction(rdd)
    val labelsAndData = rdd.map(parseFunction)
    val inter = labelsAndData.values
    val co = inter.count()
    println("RDD size: " + co)
    if (!inter.isEmpty()) {
      println("NOT EMPTY")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      val calcPCA = calculatePCA(inter, 5)
      val normalizedLabelsAndData = labelsAndData.mapValues(buildNormalizationFunction(calcPCA))
      val original = normalizedLabelsAndData.values
      original.saveAsTextFile("ds/mySKMSkafkaSample/" + dateString + "-data")
      original
    } else {
      println("Is EMPTY")
      val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
      val original = originalAndData.values
      original
    }

  }
}
