package org.utad.flopez.streamingkmeans.hdfs

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

/**
 * @author flopez 
 */
object mySkmeansHdfs {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("mySkmeansHdfs")
    val ssc = new StreamingContext(conf, Seconds(10L))
    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%
    val rawData = ssc.textFileStream("hdfs://quickstart.cloudera/user/cloudera/dset")

    val outputFolder = "ds/mySKMSHDFS/"

    clusteringTest(rawData, outputFolder, numVarPCA)

    anomaliesMain(rawData, outputFolder, numVarPCA, k)

    ssc.start()
    ssc.awaitTermination()
  }
  def toVector(rdd: RDD[String]): RDD[Vector] = {
    val parseFunction = buildLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    val original = originalAndData.values
    original
  }

  def isEmptyDStream(rawData: DStream[String]): Boolean = {
    var res: Boolean = true
    rawData.foreachRDD { rdd =>
      if (!rdd.partitions.isEmpty) {
        res = false
      }
    }
    res
  }

  // Clustering test method 1. Predictions. From Jeremy Freeman

  def clusteringTest(rawData: DStream[String], outputFolder: String, numVarPCA: Int): Unit = {

    val data: DStream[Vector] = rawData.transform(rdd => toVector(rdd))

    val model = new StreamingKMeans()
      .setK(4)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    val predictions = model.predictOn(data)
    val w = model.latestModel().clusterWeights

    predictions.foreachRDD { rdd =>
      val modelString = model.latestModel().clusterCenters
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
      val predictString = rdd.map(p => p.toString()).collect.mkString("\n")
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputFolder, dateString + "-model", modelString)
      printToFile(outputFolder, dateString + "-predictions", predictString)

    }
  }

  // Calculate anomalies from a DStream
  def anomaliesMain(rawData: DStream[String], outputFolder: String, numVarPCA: Int, k: Int) {

    rawData.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        anomalies(rdd, outputFolder, numVarPCA, k)
      }
    }
    
  }
  // Save the points that fall outside a certain radius.
  def anomalies(rawData: RDD[String], outputFolder: String, numVarPCA: Int, k: Int) = {
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val data = originalAndData.values
    val calcPCA = calculatePCA(data, 15)
    val normalizeFunction = buildNormalizationFunction(calcPCA)
    //val k = stats4K(calcPCA) // Stats wrong way
    val anomalyDetector = buildAnomalyDetector(k, data, normalizeFunction)
    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys

    anomalies.foreach { x =>
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputFolder, dateString + "-anomalies", x.toString)
    }
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
    val threshold = distances.top(10).last

    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }

}
