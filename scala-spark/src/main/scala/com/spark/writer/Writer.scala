package com.spark.writer

import com.spark.SparkEnvironment
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Amit on 03-08-2018.
  */
trait Writer {
  def write(path:String,data:DataFrame,format:String="text",partitionByColumn:String="")(implicit sparkEnv:SparkEnvironment)
}
