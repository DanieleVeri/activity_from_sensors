import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TrainingApp 
{   
    def main(args: Array[String]) 
    {
        val conf = new SparkConf()
        val ss = SparkSession.builder().config(conf).getOrCreate()
        
        // TODO: data loading agnostic
        val acc_file = "/home/dan/activity_from_sensors/activity/phones_accelerometer.csv"
        val acc_data = ss.read.format("csv").option("header", "true").load("file://" + acc_file)
        acc_data.cache()
        println(s"acceleration data loaded, partitions: ${acc_data.rdd.partitions.size}")

       // feature extraction
       acc
        
    }
}