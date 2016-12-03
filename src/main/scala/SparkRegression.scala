
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
/**
  * Created by senomoy on 14-09-2016.
  */
case class Person(rating: String, income: Double, age: Int)

object SparkRegression extends App{

  def prepareFeatures(people: Seq[Person]): Seq[Vector] = {
    val maxIncome = people map(_ income) max
    val maxAge = people map(_ age) max

    people map (p =>
      Vectors dense(
        if (p.rating == "A") 0.7 else if (p.rating == "B") 0.5 else 0.3,
        p.income / maxIncome,
        p.age.toDouble / maxAge))
  }

  def prepareFeaturesWithLabels(features: Seq[org.apache.spark.mllib.linalg.Vector]): Seq[LabeledPoint] = {
    (0d to 1 by (1d / features.length)) zip (features) map (l => LabeledPoint(l._1, l._2))
  }


}
