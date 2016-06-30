package movie

import java.util.Scanner

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by manish & akash on 29/6/16.
  */
class RecommendMovie {

  val conf = new SparkConf().setAppName("Recommendation App").setMaster("local")
  val sc = new SparkContext(conf)
  val directory = "/home/knoldus/ml-1m"
  val scanner = new Scanner(System.in)
  val numPartitions = 20
  val topTenMovies = getRatingFromUser
  val numTraining = getTrainingRating.count()
  val numTest = getTestingRating.count()
  val numValidate = getValidationRating.count()

  def getRatingRDD: RDD[String] = {

    sc.textFile(directory + "/ratings.dat")
  }

  def getMovieRDD: RDD[String] = {

    sc.textFile(directory + "/movies.dat")
  }

  def getRDDOfRating: RDD[(Long, Rating)] = {

    getRatingRDD.map { line => val fields = line.split("::")

      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  def getMoviesMap: Map[Int, String] = {

    getMovieRDD.map { line => val fields = line.split("::")

      (fields(0).toInt, fields(1))
    }.collect().toMap
  }

  def getTopTenMovies: List[(Int, String)] = {

    val top50MovieIDs = getRDDOfRating.map { rating => rating._2.product }
      .countByValue()
      .toList
      .sortBy(-_._2)
      .take(50)
      .map { ratingData => ratingData._1 }

    top50MovieIDs.filter(id => getMoviesMap.contains(id))
      .map { movieId => (movieId, getMoviesMap.getOrElse(movieId, "No Movie Found")) }
      .sorted
      .take(10)
  }

  def getRatingFromUser: RDD[Rating] = {

    val listOFRating = getTopTenMovies.map { getRating => {

      println(s"Please Enter The Rating For Movie ${getRating._2} From 1 to 5 [0 if not Seen]")
      Rating(0, getRating._1, scanner.next().toLong)
    }
    }
    sc.parallelize(listOFRating)
  }

  def getTrainingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 < 4)
      .values
      .union(topTenMovies)
      .repartition(numPartitions)
      .persist()
  }

  def getValidationRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 >= 4 && data._1 <= 6)
      .values
      .union(topTenMovies)
      .repartition(numPartitions)
      .persist()
  }

  def getTestingRating: RDD[Rating] = {

    getRDDOfRating.filter(data => data._1 > 6)
      .values
      .union(topTenMovies)
      .repartition(numPartitions)
      .persist()
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

}

object RecommendMovie extends App {

  val movieRecommendationHelper = new RecommendMovie
  val sc = movieRecommendationHelper.sc
  // Load and parse the data
  val data = sc.textFile("/home/knoldus/ratings.dat")
  val ratings = data.map(_.split("::") match { case Array(user, item, rate, timestamp) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  val movies = movieRecommendationHelper.getMovieRDD.map( _.split("::"))
    .map { case Array(movieId,movieName,genre) => (movieId.toInt ,movieName) }

  val myRatingsRDD = movieRecommendationHelper.topTenMovies
  val training = ratings.filter { case Rating(userId, movieId, rating) => (userId * movieId) % 10 <= 3 }.persist
  val test = ratings.filter { case Rating(userId, movieId, rating) => (userId * movieId) % 10 > 3}.persist


  val model = ALS.train(training.union(myRatingsRDD), 8, 10, 0.01)

  val moviesIHaveSeen = myRatingsRDD.map(x => x.product).collect().toList

  val moviesIHaveNotSeen = movies.filter { case (movieId, name) => !moviesIHaveSeen.contains(movieId) }.map( _._1)

  val predictedRates =
    model.predict(test.map { case Rating(user,item,rating) => (user,item)} ).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.persist()

  val ratesAndPreds = test.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictedRates)

  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) => Math.pow((r1 - r2), 2) }.mean()

  println("Mean Squared Error = " + MSE)

  val recommendedMoviesId = model.predict(moviesIHaveNotSeen.map { product =>
    (0, product)}).map { case Rating(user,movie,rating) => (movie,rating) }
    .sortBy( x => x._2, ascending = false).take(20).map( x => x._1)
  movieRecommendationHelper.sc.stop()
}