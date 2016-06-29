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
  val directory = "/Data/ml-20m"
  val scanner = new Scanner(System.in)
  val numPartitions = 20
  val topTenMovies = getRatingFromUser
  val numTraining = getTrainingRating.count()
  val numTest = getTestingRating.count()
  val numValidate = getValidationRating.count()

  def getRatingRDD: RDD[String] = {

    sc.textFile(directory + "/ratings.csv")
  }

  def getMovieRDD: RDD[String] = {

    sc.textFile(directory + "/movies.csv")
  }

  def getRDDOfRating: RDD[(Long, Rating)] = {

    getRatingRDD.map { line => val fields = line.split(",")

      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  def getMoviesMap: Map[Int, String] = {

    getMovieRDD.map { line => val fields = line.split(",")

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
      Rating(10001, getRating._1, scanner.next().toLong)
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

  val obj = new RecommendMovie
  val ranks = List(8, 12)
  val lambdas = List(0.1, 10.0)
  val numIters = List(10, 20)
  var bestModel: Option[MatrixFactorizationModel] = None
  var bestValidationRmse = Double.MaxValue
  var bestRank = 0
  var bestLambda = -1.0
  var bestNumIter = -1
  for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
    val model = ALS.train(obj.getTrainingRating, rank, numIter, lambda)
    val validationRmse: Double = obj.computeRmse(model, obj.getValidationRating, obj.numValidate)

    if (validationRmse < bestValidationRmse) {
      bestModel = Some(model)
      bestValidationRmse = validationRmse
      bestRank = rank
      bestLambda = lambda
      bestNumIter = numIter
    }
  }

  val myRatedMovieIds = obj.getRatingFromUser.map(_.product).collect().toSeq
  val candidates = obj.sc.parallelize(obj.getMoviesMap.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
  val recommendations = bestModel.get
    .predict(candidates.map((0, _)))
    .collect
    .sortBy(-_.rating)
    .take(50)

  var i = 1
  println("Movies recommended for you:")
  recommendations.foreach { r =>
    println("%2d".format(i) + ": " + obj.getMoviesMap(r.product))
    i += 1
  }

  obj.sc.stop()
}
