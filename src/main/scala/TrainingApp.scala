import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{Row, SparkSession, functions}

class DataEntry(row: Row) {
    val time: Long = row.getAs[String]("Arrival_Time").toLong
    val x: Float = row.getAs[String]("x").toFloat
    val y: Float = row.getAs[String]("y").toFloat
    val z: Float = row.getAs[String]("z").toFloat
    val user: String = row.getAs[String]("User")
    val model: String = row.getAs[String]("Model")
    val device: String = row.getAs[String]("Device")
    val activity: String = row.getAs[String]("gt")
}

object TrainingApp 
{
    def main(args: Array[String])
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        // TODO: data loading agnostic
        val acc_file = "/home/dan/activity_from_sensors/activity/acc00.csv"
        val gyr_file = "/home/dan/activity_from_sensors/activity/phones_gyroscope.csv"
        val acc_data = ss.read.format("csv").option("header", "true").load("file://" + acc_file)
        //val gyr_data = ss.read.format("csv").option("header", "true").load("file://" + gyr_file)
        println(s"acceleration data loaded: ${acc_data.count()} records in ${acc_data.rdd.partitions.size} partitions")
        //println(s"gyroscope data loaded: ${gyr_data.count()} records in ${gyr_data.rdd.partitions.size} partitions")

        // feature extraction
        acc_data.groupBy("User", "Device", "gt").agg(
            functions.variance("x").alias("var_x"),
            functions.variance("y").alias("var_y"),
            functions.variance("z").alias("var_z"),
            functions.covar_pop("x", "y").alias("cov_xy"),
            functions.covar_pop("x", "z").alias("cov_xz"),
            functions.covar_pop("y", "z").alias("cov_yz")
        ).show(117)

        /*
        val acc_pair = acc_data.rdd.map(row => (
            s"${row.getAs[String]("User")}#${row.getAs[String]("Device")}#${row.getAs[String]("gt")}", // key
            new DataEntry(row)))
        val acc_grouped = acc_pair.combineByKey(
            (it: DataEntry) => ListBuffer[Double](it.x.toDouble),
            (acc: ListBuffer[Double], it) => acc += it.x.toDouble,
            (acc1: ListBuffer[Double], acc2: ListBuffer[Double]) => acc1 ++ acc2).cache()

        println("=============== "+acc_grouped.first()._2.size+"  "+acc_grouped.first()._1)
        val first = Vectors.dense(acc_grouped.first()._2.toArray)
        val qwe = ss.sparkContext.parallelize(Seq(first))
        val fds = Statistics.colStats(qwe)
        println(fds.mean.toString)
        */


        scala.io.StdIn.readLine()
    }
}