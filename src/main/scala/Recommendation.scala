import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random


object Recommendation extends App {

  val scanner = new java.util.Scanner(System.in)
  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val movieLensHomeDir = "/Data/ml-20m"

  while (!scanner.next().equals("exit")) {
    val ratings = sc.textFile(movieLensHomeDir + "/ratings.csv").map { line =>
      val fields = line.split(",")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile(movieLensHomeDir + "/movies.csv").map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    }.collect.toMap

    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")

    val mostRatedMovieIds = ratings.map(_._2.product) // extract movie ids
      .countByValue // count ratings per movie
      .toSeq // convert map to Seq
      .sortBy(-_._2) // sort by rating count
      .take(50) // take 50 most rated
      .map(_._1)
    // get their ids
    val random = new Random(0)
    mostRatedMovieIds.filter(x => random.nextDouble() < 0.2)
      .map(x => (x, movies(x)))

    mostRatedMovieIds.take(10).foreach(println)

    val highestRatedMovies = ratings.map {
      case (timestamp, Rating(userId, movieId, rating)) => (movieId, (rating, 1))
    }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))


    val highestRatedCombined = ratings.map {
      case (timestamp, Rating(userId, movieId, rating)) => (movieId, rating)
    }.combineByKey(v => (v, 1),
      (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .map { case (movieId, (ratingSum, totalRating)) => (movieId, ratingSum / totalRating) }
      .sortBy({ case ((movieId, agvRating)) => agvRating }, ascending = false).take(10)


    val topTenMovies = highestRatedMovies.map { case (movieId, (ratingSum, ratingCount)) =>
      (movieId, ratingSum / ratingCount)
    }.sortBy({ case ((movieId, agvRating)) => agvRating }, ascending = false).take(10)
    topTenMovies.foreach(println)
    highestRatedCombined.take(10).foreach(println)
    println("Please Enter exit to stop")
  }
  sc.stop()
}
