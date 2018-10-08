import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gzy
  */
object TestHive {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("stockTest").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)


    val hiveContext = new HiveContext(sc)
    var ttc = hiveContext.sql("select * from tp_taxreceipt_collect")

    ttc.show()
  }
}
