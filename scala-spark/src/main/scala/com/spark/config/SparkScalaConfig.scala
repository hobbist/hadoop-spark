package com.spark.config

import org.springframework.context.annotation.{ComponentScan, Configuration}

@Configuration
@ComponentScan (basePackages = Array("com.spark.apps","com.spark"))
class SparkScalaConfig
