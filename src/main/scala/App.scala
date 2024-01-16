import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.functions.{add_months, count, max}

import java.text.DateFormat
import java.util.Date

object App extends Context {

  override val appName: String = "Spark_4_2_1"
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]) = {

    val sc = spark.sparkContext

    val avocadosRDD = sc.textFile("src/main/resources/avocado.csv")
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }
      .map(line => line.split(","))
      .map(values => Avocado(
        values(0).toInt,
        values(1),
        values(2).toDouble,
        values(3).toDouble,
        values(12),
        values(13)))


    val groupByRegionRdd = avocadosRDD.groupBy(_.region).aggregateByKey(0)(
      (count, avocados) => count + avocados.size,
      (_, _) => 0)

    print("Count avocados for region")
    print(groupByRegionRdd.foreach(row => println(f"Region: ${row._1} Count: ${row._2} ")))

    val dateForFilter = dateFormat.parse("2018-02-11")
    val filterDateRdd = avocadosRDD.filter(filerDate(_, dateForFilter))

    println(f"Avocados after: $dateForFilter")
    filterDateRdd.foreach(println(_))

    val month = avocadosRDD.filter(avocado => !StringUtils.isEmpty(avocado.date)).groupBy(a => dateFormat.parse(a.date).getMonth).aggregateByKey(0)(
      (count, avocados) => count + avocados.size,
      (_, _) => 0).sortBy(_._2, ascending=false).first()

    println(f"Popular month for avocados: ${month._1+1}")

    val maxAvgPrice = avocadosRDD.reduce((avocado, secAvocado) => {
      if (avocado.avgPrice > secAvocado.avgPrice) avocado else secAvocado
    }).avgPrice

    println(f"Max avg price: $maxAvgPrice")


    val minAvgPrice = avocadosRDD.reduce((avocado, secAvocado) => {
      if (avocado.avgPrice < secAvocado.avgPrice) avocado else secAvocado
    }).avgPrice

    println(f"Min avg price: $minAvgPrice")




    val avgByRegionRdd = avocadosRDD.groupBy(_.region).aggregateByKey(0.0)(
      (a, avocados) => avocados.map(v=>v.volume).sum.toFloat / avocados.size,
      (_, _) => 0)

    print("Avg volumes for region")
    print(avgByRegionRdd.foreach(row => println(f"Region: ${row._1} avg volume: ${row._2} ")))


  }

  def filerDate(avocado: Avocado, dateForFilter: Date) : Boolean = {

    if (StringUtils.isEmpty(avocado.date)) {
      return false
    }

    return dateFormat.parse(avocado.date).after(dateForFilter)
  }

}




