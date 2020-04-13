import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TrainingApp 
{
    def main(args: Array[String])
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()

        // TODO: take from program args
        val acc_file = "/home/dan/activity_from_sensors/data/acc00.csv"
        val gyr_file = "/home/dan/activity_from_sensors/data/gyr00.csv"

        val acc_stats = Preprocessing.with_spark_core(ss.sparkContext, acc_file)
        //val acc_stats = Preprocessing.with_spark_sql(ss, acc_file)

        println(acc_stats.count())

        scala.io.StdIn.readLine()
    }
}