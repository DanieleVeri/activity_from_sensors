import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, Row, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

// TODO: Use traits to define interface
// TODO: More meaningful names
// TODO: Double to float to reduce memory by half ?

object Preprocessing
{
    type Processed = RDD[((String, String, String, String), (Double, Double, Double, Double, Double, Double))]

    def with_spark_sql(ss: SparkSession, file_uri: String): Processed =
    {
        val acc_data = ss.read.format("csv").option("header", "true").load(file_uri)

        // feature extraction
        val time_batch = (str: String) => {
            (str.toLong / 10000).toString
        }
        val time_batch_UDF = functions.udf(time_batch)
        val acc_time_batched = acc_data.withColumn("Arrival_Time", time_batch_UDF(new ColumnName("Arrival_Time")))

        val notnull_acc_data = acc_time_batched.filter(row => row.getAs[String]("gt") != "null")

        val acc_aggregate = notnull_acc_data.groupBy("Arrival_Time", "User", "Device", "gt").agg(
            functions.variance("x").alias("var_x"),
            functions.variance("y").alias("var_y"),
            functions.variance("z").alias("var_z"),
            functions.covar_pop("x", "y").alias("cov_xy"),
            functions.covar_pop("x", "z").alias("cov_xz"),
            functions.covar_pop("y", "z").alias("cov_yz")
        )

        acc_aggregate.persist(StorageLevel.MEMORY_ONLY)

        val row_to_processed = (row: Row) => (
            (   row.getAs[String]("Arrival_Time"),
                row.getAs[String]("User"),
                row.getAs[String]("Device"),
                row.getAs[String]("gt")
            ),
            (   row.getAs[Double]("var_x"),
                row.getAs[Double]("var_y"),
                row.getAs[Double]("var_z"),
                row.getAs[Double]("cov_xy"),
                row.getAs[Double]("cov_xz"),
                row.getAs[Double]("cov_yz")
            ))
        acc_aggregate.rdd.map(row_to_processed)
    }

    def with_spark_core(sc: SparkContext, file_uri: String): Processed = {
        val acc_string = sc.textFile(file_uri)
        val acc_array = acc_string.map(row => row.split(','))
        val acc_array_notnull = acc_array.filter(arr => arr(9) != "null" && arr(0) != "Index")
        val acc_array_time_batched = acc_array_notnull.map(arr => {
            val time = arr(1).toLong / 10000 // 10 sec batch
            arr(1) = time.toString
            arr
        })
        // group by: time, user, device, activity
        val acc_grouped = acc_array_time_batched.groupBy(fields => (fields(1), fields(6),fields(8), fields(9)))
        acc_grouped.persist(StorageLevel.MEMORY_ONLY)

        val acc_mean_xyz = acc_grouped.mapValues(sample_list => {
            val sum_count = sample_list.map(arr => (arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, 1)).
                reduce((acm, it) => (acm._1 + it._1, acm._2 + it._2, acm._3 + it._3, acm._4 + 1))
            (sum_count._1 / sum_count._4, sum_count._2 / sum_count._4, sum_count._3 / sum_count._4)
        })

        val acc_var_xyz = acc_grouped.join(acc_mean_xyz).mapValues(group => {
            val sum_count = group._1.map(arr => (arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, 0.0, 0.0, 0.0, 0)).
                reduce((acm, it) => (
                    acm._1 + Math.pow(it._1 - group._2._1, 2),              // var x
                    acm._2 + Math.pow(it._2 - group._2._2, 2),              // var y
                    acm._3 + Math.pow(it._3 - group._2._3, 2),              // var z
                    acm._4 + (it._1 - group._2._1) * (it._2 - group._2._2), // cov xy
                    acm._5 + (it._1 - group._2._1) * (it._3 - group._2._3), // cov xz
                    acm._6 + (it._2 - group._2._2) * (it._3 - group._2._3), // cov yz
                    acm._7 + 1))
            (sum_count._1 / sum_count._7, sum_count._2 / sum_count._7, sum_count._3 / sum_count._7,
                sum_count._4 / sum_count._7, sum_count._5 / sum_count._7, sum_count._6 / sum_count._7)
        })

        acc_var_xyz.persist(StorageLevel.MEMORY_ONLY)
        acc_var_xyz
    }
}
