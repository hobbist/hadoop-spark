  import com.spark.writer.SparkFileReadWrite
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.{SQLContext, SparkSession}
object scalaTest extends App {
  implicit  val spark: SparkSession = SparkEnvironment.setSparkEnvironment;
  implicit  val sc: SparkContext = spark.sparkContext;
  implicit  val sqlContext: SQLContext = spark.sqlContext;
  val rw=new SparkFileReadWrite()
  val words=rw.readFile("data/input.csv",true,"text")
  //words count
  words.rdd.flatMap(line=> line.mkString.split(",")).map(word=> (word,1)).reduceByKey(_+_).saveAsTextFile("data/wordcount.txt")
}

