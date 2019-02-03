package net.jp.ytake

import java.io.File

import net.jp.ytake.Extract.ExtractResult
import org.apache.spark.sql._
import org.apache.spark.sql.types._
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
  val linkDataPath: String = "data/ml-latest-small/links.csv"

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
    val ratings = readCsv(spark, ratingDataPath)

    // csvの内容をキャッシュします
    ratings.cache

    // Number of ratings: 100836
    printf("Number of ratings: %s", ratings.count)
    print("Sample of ratings:")
    ratings.show(5)

    // 2.
    /**
      * ratingsをSELECTして抽出します
      * 初めてのDataFrameの操作をやってみよう
      */
    val selectedRatings = ratings.select(
      ratings("userId"),
      ratings("movieId"),
      ratings("rating"),
      (ratings("timestamp").cast(LongType) * 1000).alias("timestamp")
    )
    selectedRatings.show(5)

    // 3.
    // 動画リストの読み込み
    val rawMovies = readCsv(spark, movieDataPath)
    rawMovies.show(5, false)

    /**
      * 前処理として扱いやすい形式へ変更します
      */
    val extractGenres = udf((value: String) => value.toLowerCase.split('|'), ArrayType(StringType))
    rawMovies.select(
      rawMovies("movieId"),
      rawMovies("title"),
      extractGenres(rawMovies("genres")).as("genres")
    ).show(5, false)

    // 4.
    /**
      * udfを利用してカラムを追加します
      */
    rawMovies.createOrReplaceTempView("raw_movies")
    rawMovies.sqlContext.udf.register("extractYear", extractYear _)
    rawMovies.sqlContext.udf.register("extractGenres", extractGenres)
    val movies = rawMovies.sqlContext.sql(
      """
         SELECT tmp.movieId,
         tmp.extracts.title AS title,
         tmp.extracts.release_date AS release_date,
         extractGenres(genres) AS genres
        FROM (SELECT *, extractYear(title) AS extracts FROM raw_movies) AS tmp
      """
    ).toDF()
    movies.printSchema()
    movies.show(false)

    // 5.
    /**
      *  異なるDataFrameを結合させてデータを作成します
      *  物理的に違うcsvファイルを結合させています
      */
    val links = readCsv(spark, linkDataPath)
    val joinedMovieDf = movies.join(links, movies("movieId") === links("movieId"))
      .select(movies("movieId"), movies("title"), movies("release_date"), movies("genres"), links("tmdbId"))
    joinedMovieDf.count
    print("Cleaned movie data with tmdbId links:")
    joinedMovieDf.show(1000, false)
  }

  /**
    * 特定も文字列をcase classを用いて分割する
    * @param value DatFrame 任意カラムの値
    * @return
    */
  def extractYear(value: String): ExtractResult = {
    Extract.year(value)
  }

  /**
    * resources配下からトレーニング用のデータcsvファイルを取得します
    * @param path csv file
    * @return
    */
  private def getFilePath(path: String): String = {
    val fs = new File(path)
    if (!fs.exists) {
      throw new RuntimeException("machine learning data file is gone.")
    }
    fs.getAbsoluteFile.toString
  }

  /**
    * @param spark SparkSession
    * @param path csv file path
    * @return
    */
  private def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read.options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    )).csv(getFilePath(path))
  }
}
