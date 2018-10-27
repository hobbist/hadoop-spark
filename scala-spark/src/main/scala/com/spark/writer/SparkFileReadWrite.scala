package com.spark.writer


import com.spark.SparkEnvironment
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkFileReadWrite extends Writer {
    def readFile(path:String,getRdd:Boolean,format:String="text")(implicit sparkEnv:SparkEnvironment):DataFrame={
      val df=sparkEnv.spark.read.option("path",path).format(format)
      df.load()
    }
  def write(path:String,data:DataFrame,format:String="text",partitionByColumn:String="")(implicit spark:SparkEnvironment):Unit={
    if(partitionByColumn!=""){
    data.write.option("format",format).option("path",path).mode("Overwrite").partitionBy(partitionByColumn).save()
    }else{
      data.write.option("format",format).option("path",path).mode("Overwrite").save()
    }
  }
}
