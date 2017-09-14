package Applications

import org.apache.spark.sql.{DataFrame}

/**
  * Created by caimy on 2017/1/6.
  */
object WordCount {
  def compute(dataframe:DataFrame): Unit ={
    val words = dataframe.rdd.map(
      row => {
        row.toSeq.toArray
      }
    ).map((_,1)).reduceByKey(_+_).collect().foreach(println)
  }
}
