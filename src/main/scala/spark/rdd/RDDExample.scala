package spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.time.{LocalDate, Month}

object RDDExample extends App {
  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String
                    )

  val targetDate = "2018-02-11"

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  def readAvocados(filename: String): RDD[Avocado] = {
    val isHeader = (record: String) => record == "id"

    sc.textFile(filename)
      .map(line => line.split(","))
      .filter(record => !isHeader(record(0)))
      .map(record => Avocado(
        record(0).toInt,
        record(1),
        record(2).toDouble,
        record(3).toDouble,
        record(12),
        record(13)
      ))
  }

  val storesRDD2 = readAvocados("src/main/resources/avocado.csv")

  val saleAvocadoRDD = storesRDD2
    .filter(rec => rec.date > targetDate)

  implicit val ord: Ordering[(Month, Int)] = Ordering.by(e => e._2)
  val maxMonth = storesRDD2
    .filter(x => x.date.nonEmpty)
    .groupBy(x => LocalDate.parse(x.date).getMonth)
    .aggregateByKey(0)((a, b) => a + b.size, (x, y) => x + y)
    .max

  implicit val avocadoOrdering: Ordering[Avocado] = Ordering.by(e => e.avgPrice)
  val maxAvgPrice = storesRDD2.max.avgPrice
  val minAvgPrice = storesRDD2.min.avgPrice

  val avgSalesRDD = storesRDD2
    .groupBy(x => x.region)
    .aggregateByKey((0, .0))(
      (a, b) => (a._1 + b.size, a._2 + b.map(av => av.volume)
        .foldLeft(0.0)((acc, av) => acc + av)),
      (x, y) => (x._1 + y._1, x._2 + y._2))
    .map(x => (x._1, x._2._2 / x._2._1))

  println("Общее количество регионов: " + storesRDD2.groupBy(x => x.region).count())
  println("Записи о продажах авокадо, сделанные после 2018-02-11: ")
  saleAvocadoRDD.foreach(println)
  println("Месяц, который чаще всего представлен в статистике: " + maxMonth)
  println("Максимальное и минимальное значение avgPrice: " + maxAvgPrice, minAvgPrice)
  println("Cредний объем продаж (volume) для каждого региона (region): ")
  avgSalesRDD.foreach(println)

  spark.stop()
}
