package com.spark.apps

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import jobUdfs._
import scala.collection.mutable

class ContactAudit extends Job {

  override def execute(): Unit = {
    val start=System.currentTimeMillis()
    //metadata used for job
    val dt = DateTime.now()
    val windowSpec = Window.partitionBy().orderBy("last_modified_date")
    //step to create data,this can be any datasource
    val rdd = sc.parallelize(Seq(Row(new Timestamp(dt.getMillis), "CR_1", "N", "removed", "system"),
      Row(new Timestamp(dt.plusMinutes(5).getMillis), "CR_2", "Y", "applied", "kapil"),
      Row(new Timestamp(dt.plusMinutes(6).getMillis), "CR_1", "Y", "applied", "amit"),
      Row(new Timestamp(dt.plusMinutes(6).getMillis), "CR_1", "N", "removed", "amit"),
      Row(new Timestamp(dt.plusMinutes(6).getMillis), "CR_1", "N", "removed", "kapil")))

    //schema for dataset,this can be skipped in case of data retrieved from json or jdbc

    val initialSchema = StructType(StructField("last_modified_date", TimestampType) :: StructField("policy_id", StringType) ::
      StructField("active", StringType) :: StructField("reason", StringType) :: StructField("updatedBy", StringType) :: Nil)
    val tSchema = StructType(StructField("last_modified_date", TimestampType) :: StructField("added", ArrayType(StringType)) ::
      StructField("removed", ArrayType(StringType)) :: StructField("reason", StringType) :: StructField("updatedBy", StringType) :: Nil)

    val df = spark.createDataFrame(rdd, initialSchema).persist()

    var history = df.withColumn("added", lit(Array[String]())).withColumn("removed", lit(Array[String]()))

    history = history.select(
      when(history("active") === "Y", addValue(history("added"), history("policy_id"))).
        otherwise(history("added")).alias("added"),
      when(history("active") === "N", addValue(history("removed"), history("policy_id"))).
        otherwise(history("removed")).alias("removed"),
      history("last_modified_date"), history("policy_id"), history("active"), history("reason"), history("updatedBy")
    )

    history = history.groupBy(
      "last_modified_date").
      agg(("added", "collect_set"), ("removed", "collect_set"), ("reason", "max"), ("updatedBy", "max")).
      orderBy(history("last_modified_date"))

    history = spark.createDataFrame(history.rdd.map(transform), tSchema)

    history = history.withColumn("removedEnts", calculateRemovedEnts(history("added"), history("removed")))
    history = history.withColumn("currentEnts", history("added"))
    history = history.withColumn("currentEnts", getCurrentEnts(history("added"), history("removedEnts"), lag(history("currentEnts"), 1).over(windowSpec)))

    history.show()
    println(s"Time to complete contactAudit job ${System.currentTimeMillis()-start}")
  }
}


object jobUdfs {

  val addValue = udf((arr: mutable.WrappedArray[String], elem: String) => {
    var returnVal = arr
    if (!arr.contains(elem)) {
      returnVal = returnVal :+ elem
    }
    returnVal
  })

  val calculateRemovedEnts = udf((added: mutable.WrappedArray[String], removed: mutable.WrappedArray[String]) => {
    removed.filterNot(st => added.contains(st))
  })

  val getCurrentEnts = udf((added: mutable.WrappedArray[String], removed: mutable.WrappedArray[String], previous: mutable.WrappedArray[String]) => {
    var returnVal: mutable.WrappedArray[String] = mutable.WrappedArray.make(Array[String]())
    if (added != null) {
      returnVal = returnVal ++ added
    }
    if (previous != null) {
      if (removed != null) {
        returnVal = returnVal ++ previous.filterNot(st => removed.contains(st))
      }
      else {
        returnVal = returnVal ++ previous
      }
    }
    returnVal
  })

  def transform(row: Row): Row = {
    val added = flatten(row(1).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]])
    val removed = flatten(row(2).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[String]]])
    Row(row(0), added, removed, row(3), row(4))
  }

  def flatten(arr: mutable.WrappedArray[mutable.WrappedArray[String]]): mutable.WrappedArray[String] = {
    var returnVal: mutable.WrappedArray[String] = mutable.WrappedArray.make(Array[String]())
    for (a <- arr) {
      if (a.nonEmpty) {
        returnVal = returnVal ++ a
      }
    }
    returnVal
  }
}
