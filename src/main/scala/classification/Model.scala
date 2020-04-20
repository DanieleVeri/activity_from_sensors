package classification

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.sql.DataFrame

abstract class Model(val param_uri: String, val rev_label_uri: String)
{
    val model: Transformer

    val rev_label: IndexToString = IndexToString.load(rev_label_uri)

    def transform(data: DataFrame): DataFrame =
    {
        val result = model.transform(data)
        rev_label.transform(result)
    }
}

object Model
{
    def get_model(kind: String, params_uri: String, rev_label_uri: String): Model =
    {
        kind match {
            case "dt_model" =>
                new DTModel(params_uri, rev_label_uri)
            case "mlp_model" =>
                new MLPModel(params_uri, rev_label_uri)
            case _ => throw new IllegalArgumentException("Invalid model type")
        }
    }
}
