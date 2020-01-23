# Spark解决数据主键key倾斜的思路
[toc]
### 概念
> 对Spark/Hadoop这样的大数据系统来讲，数据量大并不可怕，可怕的是数据倾斜。
> 
> 何谓数据倾斜？数据倾斜指的是，并行处理的数据集中，某一部分（如Spark或Kafka的一个Partition）的数据显著多于其它部分，从而使得该部分的处理速度成为整个数据集处理的瓶颈。
> 
> 对于分布式系统而言，理想情况下，随着系统规模（节点数量）的增加，应用整体耗时线性下降。如果一台机器处理一批大量数据需要120分钟，当机器数量增加到三时，理想的耗时为120 / 3 = 40分钟，如下图所示
> 
> ![理想状态的分布式资源分布](https://upload-images.jianshu.io/upload_images/4285728-6ae8ef7dca61c3ac.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


>但是，上述情况只是理想情况，实际上将单机任务转换成分布式任务后，会有overhead，使得总的任务量较之单机时有所增加，所以每台机器的执行时间加起来比单台机器时更大。这里暂不考虑这些overhead，假设单机任务转换成分布式任务后，总任务量不变。　　
>
>但即使如此，想做到分布式情况下每台机器执行时间是单机时的1 / N，就必须保证每台机器的任务量相等。不幸的是，很多时候，任务的分配是不均匀的，甚至不均匀到大部分任务被分配到个别机器上，其它大部分机器所分配的任务量只占总得的小部分。比如一台机器负责处理80%的任务，另外两台机器各处理10%的任务，如下图所示:

> ![存在数据倾斜的资源分布](https://upload-images.jianshu.io/upload_images/4285728-3ecf6271983523a5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### 危害

当出现数据倾斜时，小量任务耗时远高于其它任务，从而使得整体耗时过大，未能充分发挥分布式系统的并行计算优势。　　

另外，当发生数据倾斜时，部分任务处理的数据量过大，可能造成内存不足使得任务失败，并进而引进整个应用失败。 　

### 实例需求
背景：在一张数据表中，可能会存在主键数据分布不均匀的情况，例如：某网站的用户访问日志，日志中存在一些ip、所在州、国家/地区、省、市、县、宽带供应商、国家/地区英文名。
    
为了能够方便下游系统的统计分析，准备增加一个国家/地区英文名的简写，提供下游系统完整的数据集。
    
## 1. 需求
根据用户访问日志，关联国家/地区信息表，将国家/地区英文名简写字段，关联到相应的记录，最后将完整数据形成新的文件。
    
 ## 2. 数据结构说明
 
### 2.1 访问日志记录表：
系统用户访问日志记录表，数据来源于网络。

> | 字段 | 描述 |
> | --- | --- |
> | ip  | IP地址 |
> | continents | 所在州 |
> | counry | 国家/地区 |
> | province | 省 |
> | city | 市 |
> | area | 区 |
> | broadband | 宽带供应商 | 
> | counry_en | 国家/地区英文 |


### 2.2 国家/地区信息表
国家/地区英文名称与英文简写对应表，数据来源于网络

>| 字段 | 描述 |
>| --- | --- |
>| counry_en  | 国家/地区英文 |
>| country_short |  国家/地区英文简写 |

## 3. 数据说明：    
### 3.1 访问日志记录表
为了后续方便，特从访问日志记录表中，随机抽取4条数据用于展示，具体数据信息如下：

| ip | continents | counry  | province | city | area | broadband | counry_en |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1.0.1.0 | 亚洲 | 中国 | 福建 | 福州 |  | 电信 | China |
| 1.0.1.0 | 亚洲 | 中国 | 福建 | 福州 |  | 电信 | China |
|1.32.0.0 | 亚洲 | 中国 | 香港|  |  |  | Hong Kong |
|27.109.128.0 | 亚洲 | 中国 | 澳门 |澳门半岛|  |  | Macao |
|27.126.221.0 |亚洲 | 中国 | 台湾 |  |  | | Taiwan |

### 3.2 国家/地区信息表
根据访问日志记录表的国家/地区信息，筛选相应的国家/地区信息，具体数据信息如下：

| counry_en | country_short |
| --- | --- |
| China | CN |
| Hong Kong | HK |
| Macao | MO |
| Taiwan | TW |

通常情况下，一个产品的访问记录会受到该产品的所在地区影响，所在地区的访问量会比其他其他地区访问量大。此次采用的数据中数据分布如下：

| 序号  | 国家地区英文名 | 记录数 |
| --- | --- | --- |
| 1 | China | 5228688 |
| 2 | Hong Kong  | 182496 |
| 3 | Macao | 3408 |
| 4 | Taiwan | 124368 |

从统计数据中可以发现，来自于中国的数据要远远高于其他地区大的访问量
## 4. 处理流程

### 4.1 通常处理流程
#### 4.1.1 实现思路
   
在不考虑数据分布情况，我们只需要将两张表通过国家/地区名称关联，整理关联结果，获取国家/地区英文简称，将国家/地区英文名称对应的简写名称填写到访问日志信息表中。

#### 4.1.2 实现代码

```
    // 1.创建spark上下文
    val spark = SparkSession  
        .builder()  
        .appName("spark data skew basic example")  
        .getOrCreate()
        
    //2.读取数据
    //2.1 访问日志信息
    val ipInfos = spark.sparkContext.textFile(args(0))

    //2.2 国家地区对应表
    var country = spark.sparkContext.textFile(args(1))

    //3 关联数据
    val countryRDD :RDD[(String,String)]= country
        .map(c =>(c.split("\\|").apply(0),c.split("\\|").apply(1)))
    
    val ipRDD:RDD[(String,String)]= ipInfos
        .map(line => (line.split("\\|").apply(7),line))
    
    val result :RDD[String]= ipRDD
        .join(countryRDD)
        .map(line =>line._2._1 +"|"+line._2._2)
    
    // 将相应的结果存储到指定的目录
    result.saveAsTextFile(args(2))

```
#### 4.1.3 执行
```
spark-submit  \
--master yarn \
--executor-memory 4G \
--executor-cores 8     \
--class com.spark.dataSkew.DataSkewBasic     original-spark-training-1.0-SNAPSHOT.jar     \
/data/ip.txt /data/country.txt /data/spark/dataSkewBasic/
```
### 4.2 优化后的处理流程
当某一个key特别多的时候，根据spark的原理 会将相同的key放到相同的task，这样China这个key对应的task文件会比较大，读取文件较多，导致计算资源倾斜到相应的task，资源不平衡 。
为了解决上述问题，可以将热键的key分割成与其他非热键key的量级相同，这样可以将计算计算资源尽可能的平均分配，从而节省计算时间。那如何将热键的key平均分配呢？一种比较常用的方法：**将key值增加随机前缀（或后缀）** 。

#### 4.2.1 具体流程

1. 选取访问日志信息中热键key
2. 将日志信息热键key值增加随机前缀
3. 将国家地区信息表的热键key数据增加随机前缀
4. 将热键key的日志信息与国家地区信息进行join
5. 将非热键key的日志信息与国家地区信息进行join
6. 将最终结果进行合并

#### 4.2.2 流程图
![数据处理流程图](https://upload-images.jianshu.io/upload_images/4285728-e6e66811698cdcf5.jpg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

#### 4.2.3 具体实现
```
//1.创建sparkContext上下文
var spark = SparkSession
    .builder()
    .appName("spark data skew optimization example")
    .getOrCreate()

//2.读取数据
val ipInfos = spark.sparkContext.textFile(args(0))

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

// 5.获取国家/地区简称
var country = spark.sparkContext.textFile(args(1))

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

//8.对数据集进行join操作

//8.1 将数据量大的key进行join
//join 结果格式：（国家/地区英文，（1，国家/地区简称））
val resultMostRdd :RDD[String]= ipMostKeyRdd
    .join(countryMostRdd)
    .map(line => (line._2._1,line._2._2))
    .map(line => line._1 +"|"+line._2)

//8.2 将数据量小的key 进行join

val resultNormalRdd: RDD[String] = ipNomarlRdd
        .join(countryNormalKeyRdd)
        .map(line =>line._2._1+"|"+line._2._2)

//9 将所有结果合并(union)
var result = resultMostRdd.union(resultNormalRdd)
//10 将所有进行存储到HDFS上
result.saveAsTextFile(args(2))
```
#### 4.2.4 执行
```
spark-submit  \
--master yarn \
--executor-memory 4G \
--executor-cores 8    \
--class com.spark.dataSkew.DataSkewOptimization     original-spark-training-1.0-SNAPSHOT.jar    \
/data/ip.txt /data/country.txt /data/spark/dataSkewOpt/
```
## 5.结果分析
### 5.1 两次执行结果比较
开启spark的history-server服务，可以查看任务的执行，主要对比两个任务的执行时间等相关内容
#### 5.1.1 执行时间
没有优化的任务执行时间为：**4.3 min** ，优化后的任务执行实践为：**3.8min**
![两个任务的执行时间 ](https://upload-images.jianshu.io/upload_images/4285728-07d91154e9b5116a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

#### 5.1.2 Job 详细信息
1. 未优化任务
该任务总共只有一个job
![basic job.png](https://upload-images.jianshu.io/upload_images/4285728-627e3d72fa0faf36.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2. 优化任务
优化后的任务共有4个job，其中前三个的job为筛选热键key的过程。
![优化任务](https://upload-images.jianshu.io/upload_images/4285728-9c6cd18a95d5476d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

#### 5.1.3 任务DAG图

1. 没有优化的任务DAG相对比较简单，总共只有三个stage，前两个stage主要为textFile，第三个stage 为join和saveAsTextFile（保存文件）
![basic detail.png](https://upload-images.jianshu.io/upload_images/4285728-68ac9a1fc63258ca.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2. 优化任务的DAG比较复杂，共有4个job，其DAG共有4个
![opt job 1.png](https://upload-images.jianshu.io/upload_images/4285728-582c72156a8e2453.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
* * *
![opt job2.png](https://upload-images.jianshu.io/upload_images/4285728-aa901ecf3e0c5dc9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
* * *
![opt job3.png](https://upload-images.jianshu.io/upload_images/4285728-1b6bbae10d5a36f3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

* * *
![opt job 4.png](https://upload-images.jianshu.io/upload_images/4285728-02096ba56348698f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
* * *

## 7.参考文献

1.  (Spark性能优化之道——解决Spark数据倾斜（Data Skew）的N种姿势)[
http://www.jasongj.com/spark/skew/]