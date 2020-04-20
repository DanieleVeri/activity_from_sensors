package classification

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}

class DTClassifier(override val seed: Long) extends
    Classifier[DecisionTreeClassificationModel](seed)
{
    override val trainer: DecisionTreeClassifier = new DecisionTreeClassifier().setSeed(seed)
}
