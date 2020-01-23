package com.spark.dataSkew

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @title: DataSkew
 * @projectName spark-training
 * @description: TODO
 * @author 毛福林
 * @date 2020/1/1610:39
 */
object DataSkewBasic {
  def main(args: Array[String]): Unit = {
    // 1.创建spark上下文
    val spark = SparkSession
      .builder()
      .appName("spark data skew basic example")
      //      .config("spark.master", "local[2]")
      .getOrCreate()

    //2.读取数据
    //2.1 访问日志信息
    val ipInfos = spark.sparkContext.textFile(args(0))
    //    var ipInfos = spark.sparkContext.textFile("data/ip.txt")
    //2.2 国家地区对应表
    var country = spark.sparkContext.textFile(args(1))
    //    var country = spark.sparkContext.textFile("data/country.txt")

    //3 关联数据
    val countryRDD :RDD[(String,String)]= country.map(c =>(c.split("\\|").apply(0),c.split("\\|").apply(1)))
    val ipRDD:RDD[(String,String)]= ipInfos.map(line => (line.split("\\|").apply(7),line))
    val result :RDD[String]= ipRDD
      .join(countryRDD)
      .map(line =>line._2._1 +"|"+line._2._2)

    // 将相应的结果存储到指定的目录
    result.saveAsTextFile(args(2))

//    result.saveAsTextFile("data/result/DataSkew/Basic")
  }
}
