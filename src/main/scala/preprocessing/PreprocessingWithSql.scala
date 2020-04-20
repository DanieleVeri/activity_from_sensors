package preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{ColumnName, Row, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

class PreprocessingWithSql(val ss: SparkSession,
                           override val time_batch:Int,
                           override val storage_level: StorageLevel) extends Preprocessing with Serializable
{
    override def extract_features(file_uri: String): Processed =
    {
        val data = ss.read.format("csv").option("header", "true").load(file_uri)

        val time_batch_lambda = (str: String) => {
            (str.toLong / time_batch).toString
        }
        val time_batch_UDF = functions.udf(time_batch_lambda)
        val time_batched = data.withColumn("Arrival_Time", time_batch_UDF(new ColumnName("Arrival_Time")))

        val notnull_data = time_batched.filter(row => row.getAs[String]("gt") != "null")

        val features = notnull_data.groupBy("Arrival_Time", "User", "Device", "gt").agg(
            functions.variance("x").alias("var_x"),
            functions.variance("y").alias("var_y"),
            functions.variance("z").alias("var_z"),
            functions.covar_pop("x", "y").alias("cov_xy"),
            functions.covar_pop("x", "z").alias("cov_xz"),
            functions.covar_pop("y", "z").alias("cov_yz"),
            functions.mean("x").alias("mean_x"),
            functions.mean("y").alias("mean_y"),
            functions.mean("z").alias("mean_z")
        )

        val row_to_processed = (row: Row) => (
            (row.getAs[String]("Arrival_Time"),
            row.getAs[String]("User"),
            row.getAs[String]("Device"),
            row.getAs[String]("gt")),
            Array(row.getAs[Double]("var_x"),
                row.getAs[Double]("var_y"),
                row.getAs[Double]("var_z"),
                row.getAs[Double]("cov_xy"),
                row.getAs[Double]("cov_xz"),
                row.getAs[Double]("cov_yz"),
                row.getAs[Double]("mean_x"),
                row.getAs[Double]("mean_y"),
                row.getAs[Double]("mean_z")))

        features.rdd.map(row_to_processed).persist(storage_level)
    }

    override def extract_streaming_features(batch: RDD[String]): RDD[(String, Array[Double])] =
    {
        val struct = StructType(
            StructField("Index", StringType) ::
            StructField("Arrival_Time", StringType) ::
            StructField("Creation_Time", StringType) ::
            StructField("x", StringType) ::
            StructField("y", StringType) ::
            StructField("z", StringType) ::
            StructField("User", StringType) ::
            StructField("Model", StringType) ::
            StructField("Device", StringType) ::
            StructField("gt", StringType) ::
            StructField("Type", StringType) ::  Nil)

        val attach_label = batch.map(s => s.split(',')).map(arr => {
            arr(6) = arr(6).concat(s"(${arr(9)})")
            arr
        })
        val df = ss.createDataFrame(attach_label.map(arr => Row.fromSeq(arr.toSeq)), struct)

        val features = df.groupBy("User").agg(
            functions.variance("x").alias("var_x"),
            functions.variance("y").alias("var_y"),
            functions.variance("z").alias("var_z"),
            functions.covar_pop("x", "y").alias("cov_xy"),
            functions.covar_pop("x", "z").alias("cov_xz"),
            functions.covar_pop("y", "z").alias("cov_yz"),
            functions.mean("x").alias("mean_x"),
            functions.mean("y").alias("mean_y"),
            functions.mean("z").alias("mean_z")
        )

        val row_to_processed = (row: Row) => (
            row.getAs[String]("User"),
            Array(row.getAs[Double]("var_x"),
                row.getAs[Double]("var_y"),
                row.getAs[Double]("var_z"),
                row.getAs[Double]("cov_xy"),
                row.getAs[Double]("cov_xz"),
                row.getAs[Double]("cov_yz"),
                row.getAs[Double]("mean_x"),
                row.getAs[Double]("mean_y"),
                row.getAs[Double]("mean_z")))

        features.rdd.map(row_to_processed).persist(storage_level)
    }
}