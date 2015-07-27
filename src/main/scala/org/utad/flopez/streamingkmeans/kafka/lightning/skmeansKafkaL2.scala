package org.utad.flopez.streamingkmeans.kafka.lightning

import java.util.Calendar
import org.apache.spark.{ Logging, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext, Duration }
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD
import org.utad.flopez.kmeans.filesOps.printToFile
import org.utad.flopez.kmeans.functions._
import kafka.serializer.StringDecoder
import scala.util.{ Random, Try }
import org.viz.lightning.Lightning

/**
 * @author flopez
 */
object skmeansKafkaL2 {
  def main(args: Array[String]) {

    // Kafka params
    val brokers = "localhost:9092"
    val topics = args.toSet

    val conf = new SparkConf().setMaster("local[4]").
      setAppName("skmeansKafkaL2").set("spark.executor.memory", "1g").
      set("spark.streaming.receiver.maxRate", "10000")

    // Create context with 5000ms  batch interval
    val ssc = new StreamingContext(conf, new Duration(5000))
    // Variables for Kmeamns and PCA
    val k = 4 // 4 Calculo de Kmeans por R
    val numVarPCA = 5 // Calculo de variables PCA con el 91% acumulado

    // Create direct kafka stream with brokers and topics.
    // Only one kafka for this example localhost:9092
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val rawData = kafkaStream.map(_._2)

    // Output folder for files
    val outputFolder = "ds/skmeansKafkaL2/"

    // Create Lightning params
    val lgn = Lightning(host = "http://mytest103.herokuapp.com")
    lgn.createSession("skmeansKafkaL2")
    val black = Array.fill(4, 3)(0)
    val scatter = lgn.scatterstreaming(x = Array(1.0, 2.0, 3.0, 4.0), y = Array(1.0, 2.0, 3.0, 4.0), size = Array(1.1))

    val model = new StreamingKMeans()
      .setK(k)
      .setHalfLife(5, "batches")
      .setRandomCenters(2, 0.0)

    var count: Long = 0

    rawData.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {
        count += rdd.count
        println("Counter size" + count)
        val parseFunction = buildCategoricalAndLabelFunction(rdd)
        val labelsAndData = rdd.map(parseFunction)
        val calcPCA = calculatePCA(labelsAndData.values, numVarPCA)
        val normalizedLabelsAndData = labelsAndData.mapValues(buildNormalizationFunction(calcPCA)).cache()

        // the value obtained is wrong
        //val kEstimated = stats4K(normalizedLabelsAndData.values)

        anomalies(rdd, outputFolder, numVarPCA, k)
        val original = normalizedLabelsAndData.values

        if (Try(model.latestModel.update(original, 4.0, "batches")).isFailure) { println("Free fall!") }

        val datax = original.map(_.apply(0)).toArray
        val datay = original.map(_.apply(1)).toArray
        val centers = model.latestModel.clusterCenters
        val modelx = centers.map(_.apply(0))
        val modely = centers.map(_.apply(1))
        val modelline = centers.map(p => Array(p.apply(0)))
        val pred = model.latestModel.predict(original).toArray

        val modelString = centers
          .map(c => c.toString.slice(1, c.toString.length - 1)).mkString("\n")
        val dateString = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", "-")

        printToFile(outputFolder, dateString + "-model", modelString)

        printToFile(outputFolder, dateString + "-modelx", modelx.deep.toString())
        printToFile(outputFolder, dateString + "-modely", modely.deep.toString())

        printToFile(outputFolder, dateString + "-datax", datax.deep.toString())
        printToFile(outputFolder, dateString + "-datay", datay.deep.toString())

        printToFile(outputFolder, dateString + "-modelline", modelline.deep.toString())
        printToFile(outputFolder, dateString + "-pred", pred.deep.toString())

        println(
          "\n\tmodelx: " + modelx.deep +
            "\n\tmodely: " + modely.deep +
            "\n\tdatax: " + datax.deep +
            "\n\tdatay: " + datay.deep +
            "\n\tpred: " + pred.deep +
            "\n\tmodelline: " + modelline.deep)

        if (Try(scatter.append(datax, datay, label = pred)).isFailure) { println(" Dont work scatter data append") }
        if (Try(scatter.append(modelx, modely, color = black)).isFailure) { println(" Dont work scatter center append") }

      }

    }

    ssc.start()
    ssc.awaitTermination()
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

  // Guarda los puntos que se salen de un radio determinado.
  def anomalies(rawData: RDD[String], outputFolder: String, numVarPCA: Int, k: Int) = {
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



