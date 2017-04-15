package collaborative.filtering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._

/**
  * Created by akash on 9/4/17.
  * Music A Gift of Nature
  */
object MusicRecommendationEngine extends App {

  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val directory = "src/main/resources"
  val rawUserArtistData = sc.textFile(directory + "/user_artist_data.txt")
  val rawArtistData = sc.textFile(directory + "/artist_data.txt")
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
  }.cache()

  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
  val recommendation = model.recommendProducts(1000002, 5)
  val recommendedProducts = recommendation.map(_.product)
  artistById.filter(tuple2 => recommendedProducts.contains(tuple2._1)).collect().toList.foreach(println)
}
