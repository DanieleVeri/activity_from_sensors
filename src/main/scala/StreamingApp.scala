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
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
        /*
        val checkpointDir = "./checkpoints"
        def createStreamingContext() = {
            val sc = new SparkContext(conf)
            val ssc = new StreamingContext(sc, Seconds(1))
            ssc.checkpoint(checkpointDir)
            ssc
        }
        val ssc = StreamingContext.getOrCreate(checkpointDir, createStreamingContext)
        */
        val classifier_params = "file:///home/dan/activity_from_sensors/params"

        // local machine
        val lines = ssc.socketTextStream("localhost", 7777) // Filter our DStream for lines with "error"
        lines.print()

        ssc.start()
        ssc.awaitTermination()
    }
}