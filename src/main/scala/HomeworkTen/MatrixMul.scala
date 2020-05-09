package HomeworkTen

/*
要求：实现矩阵的乘法计算，不允许调包
输入文件的每一行为<矩阵名 行号 列号 值>例如某行为A 1 2 4，则表示矩阵A第一行第二列的值为4
输出的结果每一行为<行号 列号 值>
 */

// 导包
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}


object MatrixMul {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("SparkHW").setMaster("local")
    val sc = new SparkContext(conf)
    val matrix = sc.textFile("matrix.txt", 1)
    val matrixA = matrix.filter(line => line.contains("A"))
    val matrixB = matrix.filter(line => line.contains("B"))
    val ItemA = matrixA.map(line => {
      val linesplite = line.split(" ")
      (linesplite(2), (linesplite(0), linesplite(1), linesplite(3))) // 列号作为key
    })
    val ItemB = matrixB.map(line => {
      val linesplite = line.split(" ")
      (linesplite(1), (linesplite(0), linesplite(2), linesplite(3))) // 行号作为key
    })
    val newItem = ItemA.join(ItemB).values.map(v => {
      (v._1._2 + " " + v._2._2, v._1._3.toInt * v._2._3.toInt)
    })
    val result = newItem.reduceByKey((x, y) => x + y)
    result.collect().foreach(x => println(x._1 + " " + x._2))
    sc.stop()
  }
}