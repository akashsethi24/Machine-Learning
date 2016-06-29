package tennis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akash on 29/6/16.
  */
trait GlobalVariables {

  val conf = new SparkConf().setAppName("Tennis App").setMaster("local")
  val sc = new SparkContext(conf)
  val directory = "/Data"

  def getSportRDD: RDD[String] = {

    sc.textFile(directory + "/sports.csv")
  }
}
