package classification

import org.apache.spark.ml.classification.DecisionTreeClassifier

class DTClassifier(override val seed: Long) extends Classifier(seed)
{
    override val trainer: DecisionTreeClassifier = new DecisionTreeClassifier().setSeed(seed)
}
