import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akash on 28/6/16.
  */
class MovieRating {

  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val movieLensHomeDir = "/Data/ml-20m"

  def getTopTenMovies: Map[Int, Double] = {

    val ratings = sc.textFile(movieLensHomeDir + "/ratings.csv").map { line =>
      val fields = line.split(",")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
    val highestRatedCombined = ratings.map {
      case (timestamp, Rating(userId, movieId, rating)) => (movieId, rating)
    }.combineByKey(v => (v, 1),
      (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .map { case (movieId, (ratingSum, totalRating)) => (movieId, ratingSum / totalRating) }
      .sortBy({ case ((movieId, agvRating)) => agvRating }, ascending = false).take(10)

    val highestRatedMovies = ratings.map {
      case (timestamp, Rating(userId, movieId, rating)) => (movieId, (rating, 1))
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    val topTenMovies = highestRatedMovies.map { case (movieId, (ratingSum, ratingCount)) =>
      (movieId, ratingSum / ratingCount)
    }.sortBy({ case ((movieId, agvRating)) => agvRating }, ascending = false).take(10)
    topTenMovies.foreach(println)
    highestRatedCombined.take(10).toMap
  }

}

object MovieRating extends App {

  val scanner = new java.util.Scanner(System.in)
  val ratingObj = new MovieRating
  ratingObj.getTopTenMovies.foreach(println)

  println("for Spark Job info try http://localhost:4040")
  println("enter exit to end")

  while (scanner.next() != "exit") {
  }
  ratingObj.sc.stop
}
