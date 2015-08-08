# PFM-Master-UTAD-FL
Evaluation exercise for postgraduate Big Data.
K-Means machine Learning over Kafka's stream. Using Apache Spark and Lightning Graph(on Heroku) server.

## Configuration 
Just only spark job needs a configuration.

### Server locations
lightning="http://mytest103.herokuapp.com/"

## Building
Everything is built on a MV Cloudera 
This project is developed using maven and scala.
For the last cases we need Kafka. Dependecies with other project named Kafka-UTAD. :)

## Samples
###	From package org.utad.flopez.kmeans: testKmeans.scala
	Examples algorithm to calculate Kmeans
###	From package org.utad.flopez.streamingkmeans.hdfs: 
	Examples algorithm to calculate streamingkmeans from hdfs files
###	From package org.utad.flopez.streamingkmeans.hdfs.lightning: testKmeans.scala
	Examples algorithm to calculate streamingkmeans from hdfs files on Heroku server using lightning APIS
###	From package org.utad.flopez.streamingkmeans.kafka: 
	Examples algorithm to calculate streamingkmeans from kafka qeue
###	From package org.utad.flopez.streamingkmeans.kafka.lightning: 
	Final exercise wih all features.


## Dependencies

### [Cloudera](http://www.cloudera.com) - cdh5.1.0 (old fashioned)

**cloudera** s open-source Apache Hadoop distribution, CDH (Cloudera Distribution Including Apache Hadoop), targets enterprise-class deployments of that technology. Cloudera says that more than 50% of its engineering output is donated upstream to the various Apache-licensed open source projects (Apache Hive, Apache Avro, Apache HBase, and so on) that combine to form the Hadoop platform.

### <a name="lightning"></a>[Lightning Graph Server](http://lightning-viz.org/) [![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/lightning-viz/lightning/tree/master)

Lightning is a data-visualization server providing API-based access to reproducible, web-based, interactive visualizations

### [Maven](https://maven.apache.org) - 3.0.4

**maven** is an open source build tool for Scala and Java projects, similar to Ant.

### [Apache Spark](http://spark.apache.org) - 1.4.0

Apache Spark is an open-source cluster computing framework originally developed in the AMPLab at UC Berkeley. In contrast to Hadoop's two-stage disk-based MapReduce paradigm, Spark's in-memory primitives provide performance up to 100 times faster for certain applications. By allowing user programs to load data into a cluster's memory and query it repeatedly, Spark is well suited to machine learning algorithms.

### [Apache Haddop](http://hadoop.apache.org) - 2.3.0

Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are commonplace and thus should be automatically handled in software by the framework.

### [Scala](http://scala-lang.org) - 2.10.5

Scala is an object-functional programming language for general software applications. Scala has full support for functional programming and a very strong static type system. This allows programs written in Scala to be very concise and thus smaller in size than other general-purpose programming languages. Many of Scala's design decisions were inspired by criticism of the shortcomings of Java

### [Kafka](http://kafka.apache.org) - 0.8.2.1

Scala is an object-functional programming language for general software applications. Scala has full support for functional programming and a very strong static type system. This allows programs written in Scala to be very concise and thus smaller in size than other general-purpose programming languages. Many of Scala's design decisions were inspired by criticism of the shortcomings of Java


### [Java ](https://www.java.com/) - Cloudera Installed Edition - 1.7.0_55-cloudera 

A general-purpose computer programming language designed to produce programs that will run on any computer system.

