package org.utad.flopez.streamingkmeans.hdfs.lightning

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
import org.viz.lightning.Lightning
import scala.util.Try

/**
 * @author flopez
 */
object mySkmeansHdfsL {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("mySkmeansHdfsL").set("spark.executor.memory", "1g").set("spark.streaming.receiver.maxRate", "10000")

    val ssc = new StreamingContext(conf, Seconds(10L))

    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%

    val rawData = ssc.textFileStream("hdfs://quickstart.cloudera/user/cloudera/dset")
    
    // Output folder to save files
    val outputFolder = "ds/mySkmeansHdfsL/"
    
    // Create DStream1
    val data: DStream[Vector] = rawData.transform(rdd => toVector(rdd))
    
    // Create Lightning params
    val lgn = Lightning(host = "http://mytest103.herokuapp.com")
    lgn.createSession("mySKMSHDFSL")
    val scatter = lgn.scatterstreaming(x = Array(1.0, 2.0, 3.0, 4.0), y = Array(1.0, 2.0, 3.0, 4.0), size = Array(1.1))
    val black = Array.fill(4, 3)(0)

    val model = new StreamingKMeans()
      .setK(4)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(data)

    val predictions = model.predictOn(data)

    predictions.foreachRDD { rdd =>
      val centers = model.latestModel().clusterCenters
      val modelString = centers
        .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
        
      val modelx = centers.map(_.apply(0))
      val modely = centers.map(_.apply(1))
      val predictString = rdd.map(p => p.toString()).collect.mkString("\n")
      
      val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")
      printToFile(outputFolder, dateString + "-model", modelString)
      printToFile(outputFolder, dateString + "-predictions", predictString)
      
      if (Try(scatter.append(x = modelx, y = modely, color = black)).isFailure) { println(" Dont work scatter append") }
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
