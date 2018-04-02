package gov.dot.sih.dmp.analytics

import org.apache.spark.sql.{SaveMode, SparkSession}

object MongoTspDataBulkLoader extends App {

  println("starting to bulk load TSP data for historical subsribers pool")

  val sparkAppName = "dot-mobile-dmp-bulkloader-spark"
  val tspHistoricalData =
  //"/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/tsp-historical/demo_seed_data.csv"
  "/Users/imuthusamy/bigdata/smart-india-hackathon/dot-mobile-dmp-analytics/data/tsp-historical/SIH-TSP-HISTORY.csv"
  val mongoDbUrlKeyIn = "spark.mongodb.input.uri"
  val mongoDbUrlKeyOut = "spark.mongodb.output.uri"

  val mongoDbUrlValue = "mongodb://127.0.0.1/test-for-sih"
  val GmLabDbUrlValue =
    "mongodb://gowtham:password@ds221339.mlab.com:21339/dot"
  val ImLabDbUrlValue =
    "mongodb://ilamparithi:mlab@ds029496.mlab.com:29496/sih"

  val targetCollection = "aadhardetails"

  val spark = SparkSession
    .builder()
    .appName(sparkAppName)
    .master("local")
    .config(mongoDbUrlKeyOut, GmLabDbUrlValue)
    .getOrCreate()

  import com.mongodb.spark._
  import spark.sqlContext.implicits._

  val inputDF = spark.read
    .option("delimiter", "\t")
    .option("header", false)
    .csv(tspHistoricalData)
    .toDF("aadhar", "name", "mobilenumber", "tsp", "lsa", "dateOfReg")

  println("size of the rdd : " + inputDF.count())

  inputDF.printSchema()
  inputDF.show(15)

  val groupedDF = inputDF
    .map { row =>
      (row.getAs[String]("aadhar") + "~" + row.getAs[String]("name"),
       row.getAs[String]("mobilenumber") + "~" + row.getAs[String]("tsp") + "~" + row
         .getAs[String]("lsa") + "~" + row.getAs[String]("dateOfReg"))
    }
    .toDF("myKey", "myValue")

  groupedDF.registerTempTable("grouped_data")
  val delimiter = "~"

  val queryString =
    "SELECT myKey,concat_ws('|', collect_list(myValue)) as myValue from grouped_data group by myKey";
  val groupedResultDF = spark.sqlContext.sql(queryString).toDF()

  groupedResultDF.printSchema()
  groupedResultDF.show()

  var seqDocs = Seq[MongoAadharMobileData]()

  val pipedDFSingleColumnRDD = groupedResultDF.rdd
    .foreach { row =>
      {
        seqDocs :+= buildMongoDocument(row.getAs[String]("myKey"),
                                       row.getAs[String]("myValue"))
      }
    }

  println("size of the constructed mongo Seq :: " + seqDocs.size)

  val docsDF = seqDocs.toDF().coalesce(1)
  docsDF.printSchema()
  println("Starting MongoDB Load")
  MongoSpark.save(
    docsDF.write
      .mode(SaveMode.Append)
      .option("collection", targetCollection))
  println("Completed MongoDB Load")

  spark.close()
  println("Closed Spark session")

  def buildMongoDocument(k: String, v: String) = {

    println(s"buildMongoDocument :: $k :: $v")

    val list = new scala.collection.mutable.ListBuffer[MongoMobileDetail]
    var array = Array[MongoMobileDetail]()
    val connections = v.split("\\|")

    println(s"values length ==> " + connections.size)
    connections.foreach({ conn =>
      {
        println(s"buildMongoDocument :: conn :: $conn")
        val connFields = conn.split("~")
        val eachMobileData =
          new MongoMobileDetail(connFields(0),
                                connFields(0).toLong,
                                connFields(1).toUpperCase,
                                connFields(2).toUpperCase,
                                connFields(3),
                                "onetime-bulk-import-from-tsps",
                                false);
        list += eachMobileData
        array :+= (eachMobileData)
      }
    })

    println(s"writeToMongo - Size Of the Array to Return :: ${array.length}")

    val keyFields = k.split("~")
    val mamd = new MongoAadharMobileData(keyFields(0),
                                         keyFields(0).toLong,
                                         keyFields(1),
                                         array,
                                         Array[String](),
                                         0)
    mamd
  }

}
