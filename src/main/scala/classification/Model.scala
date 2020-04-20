package classification

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.DataFrame

abstract class Model[T <: Transformer](val param_uri: String, val rev_label_uri: String)
{
    val model: T

    val rev_label: IndexToString = IndexToString.load(rev_label_uri)

    def transform(data: DataFrame): DataFrame =
    {
        val result = model.transform(data)
        rev_label.transform(result)
    }
}
