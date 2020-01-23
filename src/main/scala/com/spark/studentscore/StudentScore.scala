package com.spark.studentscore

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * @title: StudentScore
 * @projectName spark-training
 * @description: TODO
 * @author 毛福林
 * @date 2020/1/109:58
 */
object StudentScore {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Student Score")
      .master("local[2]")
      .getOrCreate()

    val outKeyPass = "Math_Score_Pass";
    val outKeyNotPass = "Math_Score_Not_Pass";

    val sc :SparkContext = spark.sparkContext
    val lines = sc.textFile(args(0))
    // 数据格式：
    // 200412169,gavin,male,30,0401,math=24&en=60&c=85&os=78,math=22&en=20&c=85&os=78
    // 1.使用逗号将数据进行拆分
    val studentScoreCount = lines
      .map(_.split(","))
      .filter(_.length == 7)
      .map(t => t(5))
      .map(_.split("&"))
      .filter(_.length == 4)
      .map(t => t(0))
      .map(_.split("="))
      .filter(_.length ==2)
      .map(t => if (t(1).toInt >= 60) (outKeyPass,1) else (outKeyNotPass,1) )
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))

  }

}
