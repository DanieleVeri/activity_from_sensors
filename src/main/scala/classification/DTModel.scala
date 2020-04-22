package classification

import org.apache.spark.ml.classification.DecisionTreeClassificationModel

class DTModel(override val param_uri: String,
              override val label_uri: String) extends Model(param_uri, label_uri)
{
    override val model: DecisionTreeClassificationModel = DecisionTreeClassificationModel.load(param_uri)
}
