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
import scala.util.{ Random, Try }

/**
 * @author flopez
 */
object mySkmeansHdfs2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("skmeans")
    val ssc = new StreamingContext(conf, Seconds(10L))
    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%
    val rawData = ssc.textFileStream("hdfs://quickstart.cloudera/user/cloudera/dset")

    val outputFolder = "ds/mySKMSHDFS2/"
    clusteringTest(rawData, outputFolder, 15)

    ssc.start()
    ssc.awaitTermination()
  }


  // Giorgio style. 
  def clusteringTest(rawData: DStream[String], outputFolder: String, numVarPCA: Int): Unit = {

    val model = new StreamingKMeans()
      .setK(4)
      .setHalfLife(5, "batches")
      .setRandomCenters(2, 0.0)

    var count: Long = 0

    rawData.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        count += rdd.count
        println("RDD position: " + count)
        
        val parseFunction = buildLabelFunction(rdd)
        val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
        val original = originalAndData.values
        
        val scaledData = new StandardScaler(false, true).fit(original).transform(original)

        if (Try(model.latestModel.update(scaledData, model.decayFactor, model.timeUnit)).isFailure) { println("Free fall!") }

        val centers = model.latestModel().clusterCenters

        val modelString = centers
          .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")

        val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
        printToFile(outputFolder, dateString + "-model", modelString)

      }
    }
  }

}
