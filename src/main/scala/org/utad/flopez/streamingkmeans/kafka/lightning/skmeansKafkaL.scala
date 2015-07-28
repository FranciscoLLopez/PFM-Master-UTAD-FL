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
object skmeansKafkaL {
  def main(args: Array[String]) {

    // Kafka params
    val brokers = "localhost:9092"
    val topics = args.toSet

    val conf = new SparkConf().setMaster("local[4]").
      setAppName("skmeansKafkaL").set("spark.executor.memory", "2g").
      set("spark.streaming.receiver.maxRate", "10000")

    // Create context with 100ms  batch interval
    val ssc = new StreamingContext(conf, new Duration(100))
    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val rawData = kafkaStream.map(_._2)

    rawData.print()

    val outputFolder = "ds/skmeansKafkaL/"

    // Create Lightning params
    val lgn = Lightning(host = "http://mytest103.herokuapp.com")
    lgn.createSession("skmeansKafkaL")
    val label = Array(1, 2, 3, 4)
    val scatter = lgn.scatterstreaming(x = Array(-4.0, 2.0, 3.0, 20.0), y = Array(-5.0, 2.0, 3.0, 4.0), size = Array(4.3, 4.3, 4.3, 4.3), label = label)

    val data: DStream[Vector] = rawData.transform(rdd => toVector(rdd, outputFolder, numVarPCA, k))

    val model = new StreamingKMeans()
      .setK(k)
      .setHalfLife(10, "batches")
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    val predictions = model.predictOn(data)

    predictions.foreachRDD { rdd =>

      val centers = model.latestModel().clusterCenters

      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")

      val modelx = centers.map(_.apply(0))
      val modely = centers.map(_.apply(1))
      val modelx1 = Array(1.0, 3.4, 3.4, 5.3)
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      val predictString = rdd.map(p => p.toString()).collect.mkString("\n")
      val pred = rdd.toArray()
      val c1 = Array(23, 42, 22)
      val c2 = Array(123, 142, 122)
      val c3 = Array(50, 60, 170)
      val c4 = Array(140, 140, 140)

      val color = Array(c1, c2, c3, c4)

      printToFile(outputFolder, dateString + "-model", modelString)
      printToFile(outputFolder, dateString + "-predictions", predictString)

      if (Try(scatter.append(modelx1, modely, color)).isFailure) { println(" Dont work scatter append") }
      println("Y: " + modely.deep.toString)

    }

    ssc.start()
    ssc.awaitTermination()
  }

  // 
  def toVector(rdd: RDD[String], outputFolder: String, numVarPCA: Int, k: Int): RDD[Vector] = {

    val parseFunction = buildCategoricalAndLabelFunction(rdd)
    val labelsAndData = rdd.map(parseFunction)
    val calcPCA = calculatePCA(labelsAndData.values, numVarPCA)
    val normalizedLabelsAndData = labelsAndData.mapValues(buildNormalizationFunction(calcPCA)).cache()
    // the value obtained is wrong
    //val kEstimated = stats4K(normalizedLabelsAndData.values)

    anomalies(rdd, outputFolder, numVarPCA, k)
    val original = normalizedLabelsAndData.values
    original

  }

  // Detect anomalies
  def buildAnomalyDetector(k: Int,
                           data: RDD[Vector],
                           normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(datum => distToCentroid(datum, model))
    val threshold = distances.top(100).last

    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }

  def anomalies(rawData: RDD[String], outputFolder: String, numVarPCA: Int, k: Int) = {
    if (!rawData.isEmpty()) {
      val parseFunction = buildCategoricalAndLabelFunction(rawData)
      val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
      val data = originalAndData.values
      val calcPCA = calculatePCA(data, numVarPCA)
      val normalizeFunction = buildNormalizationFunction(calcPCA)
      // Function is removed, dont get kValue right
      //val kValue = stats4K(calcPCA)
      val kValue = k

      val anomalyDetector = buildAnomalyDetector(k, data, normalizeFunction)
      val anomalies = originalAndData.filter {
        case (original, datum) => anomalyDetector(datum)
      }.keys

      anomalies.foreach { x =>
        printToFile(outputFolder, "anomalies", x.toString)
      }
    }
  }

}
