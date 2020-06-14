package com.saurav.Utilities

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.Matchers
import com.saurav.Utilities._
import org.scalatest.{ FunSuite, Matchers }
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import javax.management.ObjectName

class UtilitiesTest extends FunSuite with Matchers {

  test("check CDC for Incr Feed SCD Type2 without Marker") {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkUtilityTests")
      .getOrCreate()

    val sourcedf = spark.read.format("csv").option("header", "true").load("src/test/resources/src.csv")
    val targetdf = spark.read.format("csv").option("header", "true").load("src/test/resources/tgt.csv")
    val targetdfblank = spark.read.format("csv").option("header", "true").load("src/test/resources/tgt_blank.csv")
    val incrdf = spark.read.format("csv").option("header", "true").load("src/test/resources/incr1.csv")
    val resdf = spark.read.format("csv").option("header", "true").load("src/test/resources/res.csv")
    val resdf1 = spark.read.format("csv").option("header", "true").load("src/test/resources/res1.csv")

    val primaryColumnList = Array("PK_Column")
    val partition_col = "Partition_Column"
    import org.apache.spark.sql.functions.lit
    val targetdf1blank = targetdfblank.withColumn("start_date", lit("X")).withColumn("end_date", lit("Y"))
    val targetdf1 = targetdf.withColumn("start_date", lit("X")).withColumn("end_date", lit("Y"))
    val targetdf1md5blank = targetdfblank.withColumn("md5value", lit("X")).withColumn("start_date", lit("X")).withColumn("end_date", lit("Y"))
    val cdcschema = sourcedf.drop("Partition_Column").columns
    val finalData = Utilities.IncrementalType2NonMD5(spark, incremental_df = sourcedf, target_df = targetdf1, primary_key_col_list = primaryColumnList, partition_col, start_date = "", end_date = "")
    val finalData1 = Utilities.IncrementalType2NonMD5(spark, incremental_df = incrdf, target_df = targetdf1, primary_key_col_list = primaryColumnList, partition_col = partition_col, start_date = "", end_date = "")
    //finalData.show - Check for first day feed
    finalData.drop("start_date", "end_date").show() shouldBe
      resdf.show()
    // finalData1.show - Check for 2nd day feed
    finalData1.drop("start_date", "end_date").show() shouldBe
      resdf1.show()
  }

  test("Regex Replace Test") {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    val inputDF = spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true").
      load("src/test/resources/regexReplaceTestData.csv")

    val readDF = spark.read.format("csv").option("header", "true").load("src/test/resources/regexReplaceTestData.csv")
    import spark.implicits._
    var regexReplacedDF = Utilities.regexReplacePlatformService(inputDF, Array("Name|@"))
    regexReplacedDF.filter(col("Name").contains("@")).count() shouldBe 0

  }

}