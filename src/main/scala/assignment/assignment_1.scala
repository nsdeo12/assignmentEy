package assignment
import org.apache.spark.sql.{SparkSession,Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



object assignment_1 {
  def main(args: Array[String]) {
    val logFile = "/home/nilakantha/IdeaProjects/assignmentEy/resources/TransactionData.csv"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val df=spark.read.format("csv").option("header","true").load(logFile)

    df.createOrReplaceTempView("sqlView")
    val dfout1=spark.sql("select Sender ,count(Sender) as TotalTransaction from sqlView group by Sender")
    dfout1.show(10)
    val dfout2=spark.sql("select Receiver ,count(Receiver) as TotalTransaction from sqlView group by Receiver")
    dfout2.show(10)

    val windowSpec = Window.partitionBy("Sender")
    val dfout3=df.withColumn("TotalNumberOfTransaction",sum("TransactionAmount").over(windowSpec))
      .withColumn("AvgNumberOfTransaction",avg("TransactionAmount").over(windowSpec))

    dfout3.show(10)
    spark.stop()
  }
}
