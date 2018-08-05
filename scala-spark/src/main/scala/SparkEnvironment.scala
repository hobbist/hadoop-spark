import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object SparkEnvironment {
  def setSparkEnvironment(): SparkSession = {
    val conf=new SparkConf().setMaster("local[*]").set("spark.submit.deployMode","client").
      set("spark.hadoop.yarn.resourcemanager.hostname", "127.0.0.1").
      set("spark.hadoop.yarn.resourcemanager.address", "127.0.0.1:8032").
      setAppName("first scala spark app").set("spark.driver.host","127.0.0.1").set("spark.app.id","Console")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    spark
  }
}
