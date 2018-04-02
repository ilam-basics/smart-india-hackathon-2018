package gov.dot.sih.dmp.analytics

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.google.gson.Gson

object MobileAnalytics extends App {

  println(s"starting the spark job to build analytics from mongodb")

  val sparkAppName = "dot-mobile-dmp-analytics-spark"
  val mongoDbUrlKeyIn = "spark.mongodb.input.uri"
  val mongoDbUrlKeyOut = "spark.mongodb.output.uri"
  val mongoDbUrlValue = "mongodb://127.0.0.1/test-for-sih"
  val GmLabDbUrlValue =
    "mongodb://gowtham:password@ds221339.mlab.com:21339/dot"
  val ImLabDbUrlValue =
    "mongodb://ilamparithi:mlab@ds029496.mlab.com:29496/sih"
  val spark = SparkSession
    .builder()
    .appName(sparkAppName)
    .master("local")
    .config(mongoDbUrlKeyIn, GmLabDbUrlValue)
    .config(mongoDbUrlKeyOut, mongoDbUrlValue)
    .config("spark.mongodb.input.collection", "aadhardetails")
    .config("spark.mongodb.output.collection", "aadhardetailsOut")
    .getOrCreate()

  import com.mongodb.spark._

  val mDf = MongoSpark.load(spark.sparkContext)

  import spark.implicits._
  val jsonStringDF = mDf
    .map { x =>
      (x.toJson)
    }
    .toDF("mongo_document_as_string")

  println("Number of Documents in Collection : " + jsonStringDF.count())

  var seqDocs = Seq[MongoAadharMobileData]()

  val flattenedDf = jsonStringDF
    .flatMap { row =>
      {
        println(row.toString()); val gson = new Gson();
        val mobileAadharData =
          gson.fromJson(row.getAs[String]("mongo_document_as_string"),
                        classOf[AadharMobileData]);
        val flattenedDataFrame = flattenAadharMobileData(mobileAadharData);
        flattenedDataFrame
      }
    }
    .toDF("flattened_string")

  //flattenedDf.printSchema()
  //flattenedDf.show(1000)
  val normalisedDF = flattenedDf
    .map { fs =>
      {
        val columns = fs.getAs[String]("flattened_string") split ("\\|");
        (columns(0),
         columns(1),
         columns(2),
         columns(3),
         columns(4),
         columns(5),
         columns(6),
         columns(7))
      }
    }
    .toDF("aadhar",
          "name",
          "mobileNumber",
          "tsp",
          "region",
          "date-of-reg",
          "ip",
          "dropStatus")

  val limit = 3
  val aadharCountDF =
    normalisedDF
      .groupBy("aadhar", "name")
      .count()
      .withColumnRenamed("count", "totalConnections")
      .filter(s"totalConnections > ${limit}")
      .coalesce(1)

  val tspDF =
    normalisedDF
      .groupBy("tsp")
      .count()
      .withColumnRenamed("count", "totalConnections")
      .coalesce(1)

  val regionDF =
    normalisedDF
      .groupBy("region")
      .count()
      .withColumnRenamed("count", "totalConnections")
      .coalesce(1)

  val regionOperaDF =
    normalisedDF
      .groupBy("region", "tsp")
      .count()
      .withColumnRenamed("count", "totalConnections")
      .coalesce(1)

  /*
  val finalDf = aadharCountDF.join(
    normalisedDF,
    aadharCountDF.col("aadhar") === normalisedDF.col("aadhar")).select(aadharCountDF.col("aadhar"), aadharCountDF.col("name"), aadharCountDF.col("totalConnections") )
   */

  //aadharCountDF.show(100)

  aadharCountDF.write
    .option("header", "true")
    .csv("/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/dot-reports/limit-report/")

  tspDF.write
    .option("header", "true")
    .csv("/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/dot-reports/tsp-breakdown-report/")

  regionDF.write
    .option("header", "true")
    .csv("/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/dot-reports/region-breakdown-report/")

  regionOperaDF.write
    .option("header", "true")
    .csv("/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/dot-reports/region-operator-breakdown-report/")

  val docsDF = seqDocs.toDF().coalesce(1)

  docsDF.printSchema()

  println("Starting MongoDB Load")
  MongoSpark.save(
    docsDF.write
      .mode(SaveMode.Overwrite)
      .option("collection", "aadhardetailsNew"))
  println("Completed MongoDB Load")

  spark.close()
  println("Closed Spark session")

  def flattenAadharMobileData(aMD: AadharMobileData) = {

    val list = new scala.collection.mutable.ListBuffer[AadharMobileDataRow]
    val listNew = new scala.collection.mutable.ListBuffer[String]

    aMD.mobileDetails.foreach({ md =>
      {

        val eachMobileData = new AadharMobileDataRow(aMD._id,
                                                     aMD.name,
                                                     md._id,
                                                     md.operator,
                                                     md.region,
                                                     md.dateOfReg,
                                                     md.ipAddress,
                                                     md.dropStatus.toString)
        println("Flattening => " + eachMobileData.printRecord())
        listNew += eachMobileData.printRecord()
      }
    })

    println(s"Size Of the List to Return :: ${list.size}")
    listNew
  }

  def writeToMongo(aMD: AadharMobileData) = {

    val list = new scala.collection.mutable.ListBuffer[MongoMobileDetail]
    var array = Array[MongoMobileDetail]()

    aMD.mobileDetails.foreach({ md =>
      {
        val eachMobileData = new MongoMobileDetail(md._id,
                                                   md.number.$numberLong.toLong,
                                                   md.operator,
                                                   md.region,
                                                   md.dateOfReg,
                                                   md.ipAddress,
                                                   md.dropStatus)
        list += eachMobileData
        array :+= (eachMobileData)
      }
    })

    println(s"writeToMongo - Size Of the Array to Return :: ${array.length}")

    val mamd = new MongoAadharMobileData(aMD._id,
                                         aMD.aadharNumber.$numberLong.toLong,
                                         aMD.name,
                                         array,
                                         Array[String](),
                                         0)
    mamd

  }

}
