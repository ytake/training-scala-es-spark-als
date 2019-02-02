package net.jp.ytake

import java.io.File

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions._

/**
  * Sparkを使った機械学習のハンズオンで利用するソースコードです
  *
  * Pythonで学習する場合は他のリポジトリを参照ください
  *
  */
object SparkRecommenderApp {

  /**
    * ファイルパスは任意の環境に合わせて変更してください
    * 実際に利用する場合はconfなどで指定、もしくは引数で指定します
    */
  val ratingDataPath: String = "data/ml-latest-small/ratings.csv"
  val movieDataPath: String = "data/ml-latest-small/movies.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]")
      .appName("training-scala-es-spark-als")
      .getOrCreate
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/training-scala-es-spark-als-checkpoint")

    // 1.
    /**
      * sparkのcsv readのオプションについて
      *
      * header (default false): uses the first line as names of columns.
      * inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
      */
    // 以降はDataFrameとして扱われます
    val ratings = spark.read.options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    )).csv(getFilePath(ratingDataPath))

    // csvの内容をキャッシュします
    ratings.cache

    // Number of ratings: 100836
    printf("Number of ratings: %s", ratings.count)
    print("Sample of ratings:")
    ratings.show(5)

    // 2.
    // ratingsをSELECTして抽出します
    // 初めてのDataFrameの操作をやってみよう
    val selectedRatings = ratings.select(
      ratings("userId"),
      ratings("movieId"),
      ratings("rating"),
      (ratings("timestamp").cast(LongType) * 1000).alias("timestamp")
    )
    selectedRatings.show(5)
    // udf
    // val castToLongTimestamp = udf((value: String) => value.toLong * 1000)

    // 3.
    // 動画リストの読み込み
    val rawMovies = spark.read.options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    )).csv(getFilePath(movieDataPath))
    rawMovies.show(5, false)
  }

  // resources配下からトレーニング用のデータcsvファイルを取得します
  private def getFilePath(path: String): String = {
    val fs = new File(path)
    if (!fs.exists) {
      throw new RuntimeException("machine learning data file is gone.")
    }
    fs.getAbsoluteFile.toString
  }
}
