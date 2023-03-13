import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBSCAN {


  def DFtoPoints(Df: DataFrame): RDD[Point] = Df.drop("_c2").rdd.map(x => Point(x.toSeq.map(_.toString.toDouble).toArray))

  def FindCLS(): Unit ={

  }
  def dbscan(Df : DataFrame, Func: (Point, Point) => Double, Eps: Double, spark : SparkSession, MinPts : Long) : DataFrame = {

    //в нашем решении мы будем использовать dbscan на графе , для этого мы построим вершины на точках
      val Points : RDD[(Point , VertexId)] = DFtoPoints(Df).zipWithUniqueId()
      val Edges: RDD[Edge[(Long, Long)]] = Points.cartesian(Points).filter { case (x, y) => x != y }
      .map { x => (x._1._2, x._2._2, Func(x._1._1, x._2._1)) }.filter(_._3 <= Eps).map{x => Edge(x._1, x._2)}

      val PointsWithClusters = Graph(Points.map(_.swap), Edges)
      val clusters = PointsWithClusters.connectedComponents().vertices

      val PointsInCluster = clusters.innerJoin(Points.map(_.swap))((id, cluster, point) => (point, cluster)) //.map(x => (x._2._2, x._2._1))
      //print(clusters.count())
      val CheckForMinPts = PointsInCluster.map(_._2._2).countByValue()
      import spark.implicits._
      val ClustersDF = PointsInCluster
        .map(x => if (CheckForMinPts(x._2._2) >= MinPts) x._2._1.coords.toSeq ++  Seq(x._2._2.toString.toDouble) else (x._2._1.coords.toSeq ++  Seq(-1.toDouble)))
        .map(x => (x.head, x(1),x(2).toInt))
        .toDF()

      ClustersDF.show()
      return ClustersDF.withColumnRenamed("_3", "label")

  }

  def Normalization(pnt1: Point, pnt2: Point): Double =
    (pnt1.coords, pnt2.coords).zipped.map((x, y) => math.pow(math.abs(x - y), 2)).sum

  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder().config("spark.master", "local")
      .getOrCreate()

    val DataFrame = "src/main/scala/test.csv" //args(0).trim.toString
    val Eps = 4 // args(1).trim.toInt
    val MinPts = 3000 // args(2).trim.toInt

    var Df = spark.read.csv(DataFrame) // просто вставить имя файла
    dbscan(Df, Normalization , Eps, spark, MinPts).show(500)
  }
}

case class Point(coords: Array[Double]) extends Ordering[Point]{
  def GetDist = Math.sqrt(coords.map(x => x*x).sum)

  def compare(a: Point, b: Point) = (a.GetDist - b.GetDist).toInt
}
