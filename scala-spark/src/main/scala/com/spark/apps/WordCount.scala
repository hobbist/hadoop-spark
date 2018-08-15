package com.spark.apps
import java.io.File

import com.spark.SparkEnvironment._
import com.spark.writer.SparkFileReadWrite
import org.apache.commons.io.FileUtils

class WordCount extends Job {
  override def execute(): Unit={
   val start=System.currentTimeMillis()
   val rw=new SparkFileReadWrite()
   val words=rw.readFile(properties.get("wordcount.input.path").asInstanceOf[String],true,properties.get("wordcount.input.format").asInstanceOf[String])
   //words count
   FileUtils.deleteDirectory(new File(properties.get("wordcount.output.path").asInstanceOf[String]))
   words.rdd.flatMap(line=> line.mkString.split(",")).map(word=> (word,1)).reduceByKey(_+_).saveAsTextFile(properties.get("wordcount.output.path").asInstanceOf[String])
   println(s"Time to complete contactAudit job ${System.currentTimeMillis()-start}")
 }
}