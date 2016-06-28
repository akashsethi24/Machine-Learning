import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akash on 28/6/16.
  */
class MovieRating {

  val scanner = new java.util.Scanner(System.in)
  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val movieLensHomeDir = "/Data/ml-20m"

  def getRatings(moviesList: List[(Int, String, Double)]): List[Rating] = {
    val data = moviesList.map { movie => {
      println(s"So how much do you like to rate ${movie._2} Movie From 0 to 5 ?")
      val rating = scanner.next().toDouble
      new Rating(1001, movie._1, rating)
    }
    }
    data
  }

  def getTopTenMovies: List[(Int, String, Double)] = {

    val ratings = sc.textFile(movieLensHomeDir + "/ratings.csv").map { line =>
      val fields = line.split(",")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile(movieLensHomeDir + "/movies.csv").map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    }.collect.toMap

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

    val moviesWithName = topTenMovies.toList.map { movie => (movie._1, movies.getOrElse(movie._1, "Not Found"), movie._2)
    }
    moviesWithName
  }

}


object MovieRating extends App {


  val ratingObj = new MovieRating
  ratingObj.getRatings(ratingObj.getTopTenMovies).foreach(x => println(x.user + " - " + x.rating + " - " + x.product))

  println("for Spark Job info try http://localhost:4040")
  println("enter exit to end")

  while (ratingObj.scanner.next() != "exit") {
  }
  ratingObj.sc.stop
}
