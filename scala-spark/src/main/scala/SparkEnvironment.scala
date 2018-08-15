package com.spark
import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
trait SparkEnvironment {
  implicit val properties:Properties=new Properties()
  properties.load(new FileInputStream(new File(System.getProperty("user.dir")+"/scala-spark/src/main/resources/apps.properties")))

  private def getSparkEnvironment(): SparkSession = {
    val spark = SparkSession.builder.config(getSparkConf).getOrCreate()
    spark
  }
  implicit def getSparkConf:SparkConf={
    val conf=new SparkConf().setMaster("local[*]").set("spark.submit.deployMode","client").
      set("spark.hadoop.yarn.resourcemanager.hostname", "127.0.0.1").
      set("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:8032").
      setAppName("first scala spark app").set("spark.driver.host","127.0.0.1").set("spark.app.id","Console")
    conf
  }

  implicit val spark=getSparkEnvironment
  implicit val sc=spark.sparkContext
  implicit val sqlctx=spark.sqlContext
  implicit val conf=sc.getConf

}
