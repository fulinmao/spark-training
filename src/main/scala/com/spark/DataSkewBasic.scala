package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

/**
 * @title: DataSkewBasic
 * @projectName spark-training
 * @description: 数据大数据倾斜的一个方法
 * @author 毛福林
 * @date 2020/1/1414:15
 */
object DataSkewBasic {
  def main(args: Array[String]): Unit = {
    val randomCount = 10

    //1.创建sparkContext上下文
    var spark = SparkSession
      .builder()
      .appName("spark data skew basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    //2.读取数据
        val ipInfos = spark.sparkContext.textFile(args(0))
//    var ipInfos = spark.sparkContext.textFile("data/ip.txt")

    //3.通过取样方法，获取数据数量较多的key
    var sampleRDD: RDD[String] = ipInfos.sample(false, 0.1)
    //3.1 统计取样数据中key值的数量
    var flatRDD: RDD[(String, Int)] = sampleRDD
      .map(line =>
        (line.split("\\|").apply(7), 1))
      .reduceByKey(_ + _)

    //3.2 将抽样的数据按照出现次数排序，取出出现次数最多的单词
    var sortRDD: String = flatRDD
      .sortBy(_._2, false)
      .take(1)
      .map(_._1).apply(0)

    println("+===============================" + sortRDD)
    //china

    //4.将数据倾斜的key值随机增加前缀
    //4.1 获取数据量较多的Key对应的数据
    var ipKeyRdd: RDD[(String, String)] = ipInfos
      .map(line => (line.split("\\|").apply(7), line))
      .filter(_._1.equals(sortRDD))
    //4.2 将mostKeyRdd中的key值增加随机前缀
    var ipMostKeyRdd = ipKeyRdd.map(ipInfo => (scala.util.Random.nextInt(randomCount) + "_" + ipInfo._1, ipInfo._2))
    //4.3 获取数据量较少的key对应的数据
    var ipNomarlRdd: RDD[(String, String)] = ipInfos
      .map(line => (line.split("\\|").apply(7), line))
      .filter(!_._1.equals(sortRDD))
      .map(ipInfo => (ipInfo._1, ipInfo._2))

    println("====================ipMostKeyRdd start ===========")
    ipMostKeyRdd.collect().foreach(println)
    println("====================ipMostKeyRdd end  ===========")

    println("====================ipNomarlRdd start ===========")
    ipNomarlRdd.collect().foreach(println)
    println("====================ipNomarlRdd end  ===========")


    // 5.获取国家/地区简称
    var country = spark.sparkContext.textFile(args(1))
//    var country = spark.sparkContext.textFile("data/country.txt")

    //6.需要对country表中sortRDD的值添加前缀[0-9]
    //6.1 取出country表中sortRDD的对应的数据
    val countryRdd: RDD[(String, String)] = country
      .map(c => (c.split("\\|").apply(0), c.split("\\|").apply(1)))
      .filter(_._1.equals(sortRDD))
    // 6.2 将country中sortRDD的key值添加前缀
    val countryMostRdd: RDD[(String, String)] = countryRdd.flatMap(c => {
      val a = mutable.Map[String, String]()
      for (j <- 0 to randomCount -1) {
        a.put(j + "_" + c._1, c._2)
      }
      a
    })

    //7. 获取country中非sortRDD的数据
    val countryNormalKeyRdd :RDD[(String,String)]= country
      .map(c => (c.split("\\|").apply(0),c.split("\\|").apply(1)))
      .filter(!_._1.equals(sortRDD))
      .map(line => (line._1,line._2))

    println("====================countryMostRdd start ===========")
    countryMostRdd.collect().foreach(println)
    println("====================countryMostRdd end  ===========")

    println("====================countryNormalKeyRdd start ===========")
    countryNormalKeyRdd.collect().foreach(println)
    println("====================countryNormalKeyRdd end  ===========")

    //8.对数据集进行join操作

    //8.1 将数据量大的key进行join
    //join 结果格式：（国家/地区英文，（1，国家/地区简称））
    val resultMostRdd :RDD[String]= ipMostKeyRdd
      .join(countryMostRdd)
        .map(line => (line._2._1,line._2._2))
        .map(line => line._1 +"|"+line._2)
    println("====================resultMostRdd start ===========")
    resultMostRdd.collect().foreach(println)
    println("====================resultMostRdd end  ===========")

    //    val resultMost = resultMostRdd
    //8.2 将数据量小的key 进行join

    val resultNormalRdd: RDD[String] = ipNomarlRdd
          .join(countryNormalKeyRdd)
          .map(line =>line._2._1+"|"+line._2._2)

    println("====================resultNormalRdd start  ===========")
    resultNormalRdd.collect().foreach(println)
    println("====================resultNormalRdd end  ===========")

    //9 将所有结果合并(union)
    var result = resultMostRdd.union(resultNormalRdd)
    //10 将所有进行存储到HDFS上
    result.saveAsTextFile(args(2))
//    result.saveAsTextFile("data/result/")
  }
}
