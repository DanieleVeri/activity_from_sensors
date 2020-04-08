import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object StreamingApp 
{   
    def main(args: Array[String]) 
    {
        val conf = new SparkConf()
        val checkpointDir = "./checkpoints"
        def createStreamingContext() = {
            val sc = new SparkContext(conf)
            val ssc = new StreamingContext(sc, Seconds(1))
            ssc.checkpoint(checkpointDir)
            ssc
        }
        val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
        /*
        val logFile = "/home/dan/test_spark/src/main/scala/SimpleApp.scala" // Should be some file on your system
        val logData = sc.textFile("file://"+logFile).cache()
        val numAs = logData.filter(line => line.contains("a")).count()
        val numBs = logData.filter(line => line.contains("b")).count()
        println(s"Lines with a: $numAs, Lines with b: $numBs")
        */
        ssc.stop()
    }
}