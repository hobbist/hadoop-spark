package com.spark.apps

import com.spark.SparkEnvironment
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
@Component
abstract class Job {
  @Autowired
  implicit var env:SparkEnvironment=null

  def execute():Unit

}
