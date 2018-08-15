package com.spark.apps

import com.spark.SparkEnvironment

abstract class Job extends SparkEnvironment{

  def execute():Unit

}
