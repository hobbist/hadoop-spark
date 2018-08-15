  import com.spark.apps.{ContactAudit, WordCount}
  object scalaTest extends App {
  val start=System.currentTimeMillis()
    new WordCount().execute()
   new ContactAudit().execute()
    println(s"time to complete all jobs ${System.currentTimeMillis()-start}")
  }