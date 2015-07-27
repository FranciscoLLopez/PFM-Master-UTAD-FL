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
object mySkmeansHdfs2L {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("mySkmeansHdfs2L").set("spark.executor.memory", "1g").set("spark.streaming.receiver.maxRate", "3800")

    val ssc = new StreamingContext(conf, Seconds(10L))
    
    val k = 4 //  Kmeans calculation by R
    val numVarPCA = 5 // PCA calculation variables with accumulated 91%
    
    val rawData = ssc.textFileStream("hdfs://quickstart.cloudera/user/cloudera/dset")
    // Output folder to save files
    val outputFolder = "ds/mySkmeansHdfs2L/"
    
    // Create Lightning params
    val lgn = Lightning(host = "http://mytest103.herokuapp.com")
    lgn.createSession("mySkmeansHdfs2L")
    val black = Array.fill(4, 3)(0)
    val scatter = lgn.scatterstreaming(x = Array(1.0, 2.0, 3.0, 4.0), y = Array(1.0, 2.0, 3.0, 4.0), size = Array(1.1), color = black)

    val model = new StreamingKMeans()
      .setK(k)
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

        if (Try(model.latestModel.update(original, 4.0, "batches")).isFailure) { println("Free fall!") }
   
        val datax = scaledData.map(_.apply(0)).toArray
        val datay = scaledData.map(_.apply(1)).toArray
        val centers = model.latestModel.clusterCenters
        val modelx = centers.map(_.apply(0))
        val modely = centers.map(_.apply(1))
        val modelline = centers.map(p => Array(p.apply(0)))
        val pred = model.latestModel.predict(scaledData).toArray

        if (Try(scatter.append(x = datax, y = datay, label = pred)).isFailure) { println(" Dont work scatter append predictions") }

        if (Try(scatter.append(x = modelx, y = modely, color = black)).isFailure) { println(" Dont work scatter append centers") }

        val modelString = centers.map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
        val predictString = rdd.map(p => p.toString()).collect.mkString("\n")

        val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")

        printToFile(outputFolder, dateString + "-model", modelString)
        printToFile(outputFolder, dateString + "-predictions", predictString)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
