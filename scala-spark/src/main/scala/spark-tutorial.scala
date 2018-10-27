  import com.spark.apps.{ContactAudit, WordCount}
  import com.spark.config.SparkScalaConfig
  import org.springframework.context.annotation.AnnotationConfigApplicationContext
  object scalaTest extends App {
  val start=System.currentTimeMillis()
  val ctx=new AnnotationConfigApplicationContext(classOf[SparkScalaConfig])
   ctx.getBean(classOf[WordCount]).execute()
    ctx.getBean(classOf[ContactAudit]).execute()
    println(s"time to complete all jobs ${System.currentTimeMillis()-start}")
  }