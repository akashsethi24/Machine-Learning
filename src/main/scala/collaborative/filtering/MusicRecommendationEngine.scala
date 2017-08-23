package collaborative.filtering

import java.util.Scanner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD

/**
  * Created by akash on 9/4/17.
  * Music A Gift of Nature
  */
object MusicRecommendationEngine extends App {

  val scanner = new Scanner(System.in)

  def getTopTenArtist(rawUserArtistData: RDD[String]): Array[(Int, Int)] = {
    rawUserArtistData.map { line =>
      val array = line.split(" ")
      val artistId = array(1)
      val playCount = array(2).toInt
      (artistId.toInt, playCount)
    }.reduceByKey(_ + _).sortBy(-_._2).take(10)
  }

  def getArtistName(rawArtistData: RDD[String]): RDD[(Int, String)] = {
    rawArtistData.map {
      line =>
        val array = line.replaceAll("\\s+", " ").split(" ")
        val artistId = try {
          array(0).toInt
        } catch {
          case exception: Exception => 9999
        }
        val artistName = try {
          array(1)
        } catch {
          case exception: Exception => ""
        }
        (artistId, artistName)
    }
  }

  def getArtistData: RDD[(Int, String)] = {
    val topTenArtistRDD = sc.parallelize(getTopTenArtist(rawUserArtistData))
    topTenArtistRDD.join(getArtistName(rawArtistData)).map { data =>
      (data._1, data._2._2)
    }
  }

  def getRatingFromUser: RDD[Rating] = {
    val data = getArtistData
    data.collect().toList.foreach(println)
    data.map {
      case (movieId, movie) =>
        println(s"Please Enter The Rating For Movie $movie From 1 to 5 [0 if not Seen]")
        Rating(0, movieId.toInt, scanner.next().toLong)
    }
  }

  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val directory = "src/main/resources"
  val rawUserArtistData = sc.textFile(directory + "/user_artist_data.txt")
  val rawArtistData = sc.textFile(directory + "/artist_data.txt")
  val ratingFromUser = getRatingFromUser

  val artistById = rawArtistData.map { line =>
    val dataArray = line.split("\t")
    try {
      (dataArray(0).trim.toInt, dataArray(1).trim)
    } catch {
      case exception: Exception => (0, "")
    }
  }.filter(_._1 != 0)
  val rawArtistAlias = sc.textFile(directory + "/artist_alias.txt")
  val artistAlias = rawArtistAlias.flatMap { line =>
    val tokens = line.split('\t')
    if (tokens(0).isEmpty) {
      None
    } else {
      Some((tokens(0).toInt, tokens(1).toInt))
    }
  }.collectAsMap()

  val bArtistAlias = sc.broadcast(artistAlias)
  val trainData = rawUserArtistData.map { line =>
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID =
      bArtistAlias.value.getOrElse(artistID, artistID)
    Rating(userID, finalArtistID, count)
  }.union(ratingFromUser).cache()

  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
  val recommendation = model.recommendProducts(0, 10)
  val recommendedProducts = recommendation.map(_.product)
  artistById.filter(tuple2 => recommendedProducts.contains(tuple2._1)).collect().toList.foreach(println)
}
