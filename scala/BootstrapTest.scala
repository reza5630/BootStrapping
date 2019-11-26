import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BootstrapTest extends App {


  override def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("Data Analysis").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dataset: RDD[String] = sc.textFile("src/main/resources/computers.csv")

    val keyValueRDD: RDD[(String, Double)] = dataset.map(line => (line.split(",")(4),
      line.split(",")(1).toDouble))

    val resultFromDataset = meanVarCal(sc, keyValueRDD)

    val sampleData = keyValueRDD.sample(false, .25)
    for (x <- sampleData)
      println(x)

    var resultfromResampling: List[List[(String, (Double, Double))]] = List()

    val numberOfIter = 20
    for (x <- 0 to numberOfIter - 1)
      resultfromResampling = resultfromResampling :+ meanVarCal(sc, sampleData.sample(true, 1))


    var temp: List[(String, (Double, Double))] = List(("C", (0, 0)), ("C", (0, 0)), ("C", (0, 0)),
      ("C", (0, 0)), ("C", (0, 0)), ("C", (0, 0)))
    for (x <- resultfromResampling) {
      temp = List((x(0)._1, (temp(0)._2._1 + x(0)._2._1, temp(0)._2._2 + x(0)._2._2)),
        (x(1)._1, (temp(1)._2._1 + x(1)._2._1, temp(1)._2._2 + x(1)._2._2)),
        (x(2)._1, (temp(2)._2._1 + x(2)._2._1, temp(2)._2._2 + x(2)._2._2)),
        (x(3)._1, (temp(3)._2._1 + x(3)._2._1, temp(3)._2._2 + x(3)._2._2)),
        (x(4)._1, (temp(4)._2._1 + x(4)._2._1, temp(4)._2._2 + x(4)._2._2)),
        (x(5)._1, (temp(5)._2._1 + x(5)._2._1, temp(5)._2._2 + x(5)._2._2)))
    }

    temp = List((temp(0)._1, (temp(0)._2._1 / numberOfIter, temp(0)._2._2 / numberOfIter)),
      (temp(1)._1, (temp(1)._2._1 / numberOfIter, temp(1)._2._2 / numberOfIter)),
      (temp(2)._1, (temp(2)._2._1 / numberOfIter, temp(2)._2._2 / numberOfIter)),
      (temp(3)._1, (temp(3)._2._1 / numberOfIter, temp(3)._2._2 / numberOfIter)),
      (temp(4)._1, (temp(4)._2._1 / numberOfIter, temp(4)._2._2 / numberOfIter)),
      (temp(5)._1, (temp(5)._2._1 / numberOfIter, temp(5)._2._2 / numberOfIter)))

    var seq: Seq[Seq[Any]] = Seq()
    seq = seq:+Seq("Category "+"\""+temp(0)._1+"\"", "Category "+"\""+temp(1)._1+"\"", "Category "+"\""+temp(2)._1+"\"",
      "Category "+"\""+temp(3)._1+"\"", "Category "+"\""+temp(4)._1+"\"", "Category "+"\""+temp(5)._1+"\"")
    for (x <- 0 to numberOfIter - 1) {
      seq = seq:+Seq(resultfromResampling(x)(0)._2,
        resultfromResampling(x)(1)._2, resultfromResampling(x)(2)._2,
        resultfromResampling(x)(3)._2, resultfromResampling(x)(4)._2,
        resultfromResampling(x)(5)._2)
    }

    seq = seq:+Seq("Final: "+temp(0)._2, "Final: "+temp(1)._2, "Final: "+temp(2)._2,
      "Final: "+temp(3)._2, "Final: "+temp(4)._2, "Final: "+temp(5)._2)
    seq = seq:+Seq("Original: "+resultFromDataset(0)._2, "Original: "+resultFromDataset(1)._2, "Original: "+resultFromDataset(2)._2,
      "Original: "+resultFromDataset(3)._2, "Original: "+resultFromDataset(4)._2, "Original: "+resultFromDataset(5)._2)


    println(formatTable(seq))

  }

  def meanVarCal(sc: SparkContext, keyValueRDD: RDD[(String, Double)]): List[(String, (Double, Double))] = {
    val valMapped = keyValueRDD.mapValues(x => (x, 1.0))

    val reduced = valMapped.reduceByKey((k, v) => (k._1 + v._1, k._2 + v._2)).collect()

    val meanWithN = sc.parallelize(reduced).mapValues((v => (calMean(v._1, v._2), v._2))).collect()

    var keyValueGroupByKey = keyValueRDD.groupByKey().collect().map(x => (x._1, x._2.toList: List[Double]))

    val combinedWithMean = keyValueGroupByKey ++ meanWithN
    val combinedWithMeanStr = sc.parallelize(combinedWithMean).groupByKey().map(x => (x._1, x._2.toList)).sortByKey().collect()
    //val res: mutable.MutableList[(String, (Double, Double))] = mutable.MutableList()
    var result: List[(String, (Double, Double))] = List()

    for (cat <- combinedWithMeanStr) {
      val list: List[Double] = cat._2(0).asInstanceOf[List[Double]]

      def sqr(x: Double) = (x - toList(cat._2(1))(0)) * (x - toList(cat._2(1))(0))

      val v = list.map(sqr)
      //println(cat._1+" "+ toList(cat._2(1))(0)+" "+ calVar(v, toList(cat._2(1))(1)))
      result = result :+ (cat._1, (toList(cat._2(1))(0), calVar(v, toList(cat._2(1))(1))))
    }
    return result
  }

  def toList(tuple: Product): List[Double] = tuple.productIterator.map(_.asInstanceOf[Double]).toList

  def calMean(total: Double, count: Double): Double = {
    return total / count
  }

  def calVar(devList: List[Double], count: Double): Double = {
    return devList.sum / count
  }

  def formatTable(table: Seq[Seq[Any]]): String = {
    if (table.isEmpty) ""
    else {
      // Get column widths based on the maximum cell width in each column (+2 for a one character padding on each side)
      val colWidths = table.transpose.map(_.map(cell => if (cell == null) 0 else cell.toString.length).max + 2)
      // Format each row
      val rows = table.map(_.zip(colWidths).map { case (item, size) => (" %-" + (size - 1) + "s").format(item) }
        .mkString("|", "|", "|"))
      // Formatted separator row, used to separate the header and draw table borders
      val separator = colWidths.map("-" * _).mkString("+", "+", "+")
      // Put the table together and return
      (separator +: rows.head +: separator +: rows.tail :+ separator).mkString("\n")
    }
  }
}