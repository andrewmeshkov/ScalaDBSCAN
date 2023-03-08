import breeze.linalg.max
import org.apache.commons.logging.Log
import org.apache.parquet.format.IntType
import org.apache.spark
import org.apache.spark._

import scala.collection.immutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.{:+, io}
import scala.math.Ordering.{Double, Tuple3, comparatorToOrdering, ordered}
import scala.util.control.Breaks.{break, breakable}


object dbscan {

  implicit def ordering[A <: Double]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      x.compareTo(y)
    }
  }


  def dataframe_to_points_row(df: DataFrame): RDD[Point] = df.drop("_c2").rdd.map(x => Point(x.toSeq.map(_.toString.toDouble).toArray))


  def dbscan(df : DataFrame, func: (Point, Point) => Double, epsilon: Double, spark : SparkSession) : DataFrame = {

    //в нашем решении мы будем использовать dbscan на графе , для этого мы построим вершины на точках
      val point : RDD[(VertexId , Point)] = dataframe_to_points_row(df).zipWithUniqueId().map(_.swap)
      val vertex_ids = Map(point.collect().toList: _*)
      print(vertex_ids)
      val edges: RDD[Edge[(Long, Long)]] = point.map(_._1).cartesian(point.map(_._1)).filter { case (x, y) => x != y }
      .map { x => (x._1, x._2, func(vertex_ids(x._1), vertex_ids(x._2))) }.filter(_._3 <= epsilon).map{x => Edge(x._1, x._2)}


      val points_wth_clusters = Graph(point, edges)
      val clusters = points_wth_clusters.connectedComponents().vertices

      val points_in_cluster = clusters.innerJoin(point)((id, cluster, point) => (cluster, point))//.map(x => (x._2._2, x._2._1))
      val map_of_clusters = Map(points_in_cluster.map(x => (x._1, x._2._1)).collect().toList: _*)
      import spark.implicits._
      val clusters_df = point.map(_._1)
        .map(x => vertex_ids(x).coords.toSeq ++  Seq(map_of_clusters(x).toDouble)).map(x => (x.head, x(1),x(2).toInt)).collect().toSeq.toDF()
      clusters_df.show()
      return clusters_df.withColumnRenamed("_3", "label")

  }

  def normaliation(pnt1: Point, pnt2: Point): Double =
    (pnt1.coords, pnt2.coords).zipped.map((x, y) => math.pow(math.abs(x - y), 2)).sum

  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder().config("spark.master", "local")
      .getOrCreate()

    val dataframe = "src/main/scala/test.csv" //args(0).trim.toString
    val epsilon = 2 // args(1).trim.toInt

    var df = spark.read.csv(dataframe) // просто вставить имя файла
    dbscan(df, normaliation , epsilon, spark).show(130)
  }
}

case class Point(coords: Array[Double])
