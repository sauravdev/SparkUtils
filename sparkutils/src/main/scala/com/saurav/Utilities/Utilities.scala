package com.saurav.Utilities

import java.net.URI
import scala.reflect.api.materializeTypeTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkEnv
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.length
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.apache.spark.sql.functions.broadcast

object Utilities {

  /**
   *
   * Logger will be used for application specific logs
   *
   */
  val logger = Logger.getLogger(getClass.getName.replaceAll("\\$$", ""))
  def caller = Thread.currentThread.getStackTrace()(3).getClassName.replaceAll("\\$$", "")

  //Trim Service

  /**
   * Trimming Service will trim data based on Metadata Table
   * @param inputDF : Input Dataframe
   * @param schemaList : SChema based on metadata table for trimming
   * @return Trimmed Dataframe
   */
  def trimDF(inputDF: DataFrame, schemaList: Array[String]): DataFrame = {
    var trimmedDf: DataFrame = inputDF
    try {
      for (columnDetails <- schemaList) {
        var columnName = inputDF { columnDetails.split('|')(0) }
        trimmedDf = trimmedDf.withColumn(columnName.toString(), lit(trim(col(columnName.toString()))))
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to execute SQL please check yarn logs with applicationId " + SparkEnv.get.conf.getAppId)
        println(e.printStackTrace())
        throw e

    }
    return trimmedDf

  }

  /**
   * Service will replace regexs from the columns based on the regex mentioned in metadata table
   * @param inputDF : Input Dataframe
   * @param columnRegexList : Columns and Regex which need to be replaced
   * @return : Dataframe
   */
  def regexReplacePlatformService(inputDF: DataFrame, columnRegexList: Array[String]): DataFrame = {
    var regexReplaceDF: DataFrame = inputDF
    try {
      for (columnRegexDetails <- columnRegexList) {

        var columnName = inputDF { columnRegexDetails.split('|')(0) }
        var colRegex = columnRegexDetails.split('|')(1)
        regexReplaceDF = regexReplaceDF.withColumn(columnName.toString(), lit(regexp_replace(columnName, colRegex, "")))

      }
    } catch {
      case e: Exception =>
        logger.error("please check yarn logs with applicationId " + SparkEnv.get.conf.getAppId)
        println(e.printStackTrace())
        throw e

    }
    return regexReplaceDF

  }

  /**
   * Extract time component and fetch modification time of file
   * @param inputPath : Input Path
   * @param hadoopConfiguration : Hadoop Configuration
   * @return : Modification time as String
   */
  def extractTimeComponent(inputPath: String, hadoopConfiguration: Configuration): String = {
    val map = scala.collection.mutable.Map[String, Long]()
    val fs = FileSystem.get(new URI(inputPath), hadoopConfiguration)
    val status = fs.listStatus(new Path(inputPath))
    status.foreach(x => if (x.isDirectory()) (map(x.getPath.toString()) = x.getModificationTime))
    val lstMap = map.toList.sortWith((x, y) => x._2 > y._2)
    val hd = lstMap.head
    val key = hd._1
    val splitStr = key.split("/").last
    splitStr.split("=").last
  }

  /**
   * getCurrentTimestamp function returns the current timestamp in ISO format
   * @return Time stamp in string format
   */
  def getCurrentTimestamp(): String = {
    val ISOFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    val now: DateTime = new org.joda.time.DateTime()
    ISOFormatGeneration.print(now)
  }

  /**
   * Utility method will use to read properties file
   * @param inputPath - Input path of the Properties file
   * @param hdfsUri - HDFS Uri
   * @return
   */
  def readProperties(inputPath: String, hdfsUri: Option[String] = None): OrderedProperties = {
    val logger = Logger.getLogger(getClass.getName)
    logger.info("READING PROPERTIES FILE @@@@@@@@@@@@@@@@@@@@@@")
    val hdpConf = new Configuration
    hdfsUri match {
      case Some(hdfsUri) =>
        hdpConf.set("fs.defaultFS", hdfsUri)
      case _ =>
    }
    val fs = FileSystem.get(hdpConf)
    Try {
      logger.info("LOADING PROPERTIES FILE @@@@@@@@@@@@@@@@@@@@@@" + inputPath)
      val propFileInputStream = fs.open(new Path(inputPath))
      logger.info("READ PROPERTIES FILE @@@@@@@@@@@@@@@@@@@@@@")
      var properties = new OrderedProperties()
      logger.info("CREATED PROPERTIES OBJECT @@@@@@@@@@@@@@@@@@@@@@")
      properties.load(propFileInputStream)
      logger.info("LOADED PROPERTIES FILE @@@@@@@@@@@@@@@@@@@@@@")
      properties
    } match {
      case Failure(fail) => { throw new Exception(fail.getCause) }
      case Success(succ) => succ
    }
  }

  /**
   * convertDateTime function takes all the dateFields which are needed to be converted to ISO format
   * @param validDF			: Valid records which passed the validateColumnLength Check
   * @param dateFields	: List of Hive table columns read  on which validation is to be performed
   * @return List[DataFrame] : List of dataframe of valid and invalid records.
   */
  def convertDateTime(validDF: DataFrame, dateFields: Array[String]): DataFrame = {
    var validDFDate: DataFrame = validDF;
    try {
      println("Inside convertDateTime")
      logger.info(" LOGGER se : Inside convertDateTime")
      val mydate_udfdd = udf(convertDateTimeUDF _)
      for (columns <- dateFields) {
        var dateField = columns.split('|')(0)
        var dateFormat = columns.split('|')(1)
        validDFDate = validDFDate.withColumn(dateField, mydate_udfdd(validDF { dateField }, lit(dateFormat)))
      }

    } catch {
      case e: Exception =>
        logger.error("Unable to CovertDateTime please check yarn logs for exact errror with applicationId " + SparkEnv.get.conf.getAppId)
        e.printStackTrace()
        throw e
    }

    return validDFDate
  }

  /**
   * Utility method will conevert input date of string type to given format
   * @param inputDate : Input date which needs to be converted
   * @param inputDateFormat: Required Input date format
   * @return
   */
  def convertDateTimeUDF(inputDate: String, inputDateFormat: String): String = {
    try {
      val isoFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
      val dateFormatGeneration: DateTimeFormatter = DateTimeFormat.forPattern(inputDateFormat)
      val jodatime: DateTime = dateFormatGeneration.parseDateTime(inputDate);
      val dateString: String = isoFormatGeneration.print(jodatime);
      return dateString
    } catch {
      case e: Exception =>
        logger.error("Unable to CovertDateTime please check yarn logs for exact errror with applicationId " + SparkEnv.get.conf.getAppId)
        e.printStackTrace()
        throw e
        return ""
    }

  }

  //Case class to hold file path and its last modified timestamp
  case class DataList(filePath: String, mod_ts: Long)

  /**
   * getSortedFileObjects method lists the file name in a HDFS folder
   * @param sparkContext	: Spark Context
   * @param path					: Input path of the folder
   * @return							: List of files
   */

  def getSortedFileObjects(spark: SparkSession, path: String, fs: FileSystem): List[DataList] = {
    fs.listStatus(new Path(path)).map { x => DataList(x.getPath.toString(), x.getModificationTime.toLong) }.toList.sortBy { files => files.mod_ts }
  }

  /**
   * sortedFiles method sorts the file based on modified timestamp of file.
   * @param sparkContext	: Spark Context
   * @param path					: Input path of the folder
   * @return							: List of files
   */

  def sortedFiles(spark: SparkSession, path: String, last_modified_ts: Long, hdfsUri: String): List[String] = {
    val hdpConf = new Configuration
    if (!(hdfsUri.trim().isEmpty() || hdfsUri == null)) { hdpConf.set("fs.defaultFS", hdfsUri) }
    val fs = FileSystem.get(hdpConf)
    var finalList: List[DataList] = List[DataList]()
    var returnList: List[DataList] = List[DataList]()
    val listOfDirectories = fs.listStatus(new Path(path)).filter(x => x.isDirectory() == true).map { x => x.getPath.toString() }.toList
    if (listOfDirectories.length > 0) {
      listOfDirectories.foreach { directories =>
        var newList = getSortedFileObjects(spark, directories, fs)
        finalList = finalList ::: newList
      }
      returnList = finalList.sortBy(files => files.mod_ts).filter(_.mod_ts > last_modified_ts)
    } else {
      returnList = getSortedFileObjects(spark, path, fs)
    }
    val r = returnList.map { x => x.filePath }: List[String]
    return r
  }

  /**
   * @param spark : Spark session
   * @param incremental_file_list : List of files
   * @param hdfsUri : HDFS Uri
   * @return : Latest File Name
   */
  def getlatestfilets(spark: SparkSession, incremental_file_list: List[String], hdfsUri: String): Long = {
    val latesfile = incremental_file_list.last
    val hdpConf = new Configuration
    if (!(hdfsUri.trim().isEmpty() || hdfsUri == null)) { hdpConf.set("fs.defaultFS", hdfsUri) }
    val fs = FileSystem.get(hdpConf)
    val dirPath = new Path(latesfile)
    val filestatus = fs.listStatus(dirPath)
    val lst = filestatus.map(i => i.getModificationTime())
    var lastmodtime = lst(0)
    if (lastmodtime.equals(null)) lastmodtime = 0
    lastmodtime
  }

  def sequenceIDGenerator(str: String): Long = {

    import java.math.BigInteger
    var number = ""
    str.map(_.asDigit).foreach { i =>
      number = number + i
    }
    return (new BigInteger(number).longValue())
  }

  /**
   * generateSequenceId method lists the file name in a HDFS folder
   * @param df	: DataFrame
   * @param list	: List[String]
   * @return	: DataFrame
   */
  def generateSequenceId(df: DataFrame, list: List[String], spark: SparkSession): DataFrame = {
    var natural_key_columns = ""
    list.foreach { x =>
      natural_key_columns = natural_key_columns + "," + x
    }
    natural_key_columns = natural_key_columns.drop(1)
    spark.udf.register("udf_sequenceIDGenerator", sequenceIDGenerator _)
    df.registerTempTable("raw_table")
    var sequenceIDGenerator_sql = f"""select udf_sequenceIDGenerator($natural_key_columns) as sequence_ID,* from raw_table"""
    println("Date Check Query:" + sequenceIDGenerator_sql)
    val seqId_df = spark.sql(sequenceIDGenerator_sql)
    spark.sql("drop table if exists raw_table")
    return seqId_df
  }

  /**
   * validateNotNull function checks whether the primary key column has
   *  Null values and if any, loads them to the error table
   *
   * @param SQLContext
   * @param DataFrame which contains the input table data
   * @param List which contains the primary key columns read from the yaml file
   * @return DataFrame of valid and invalid records
   */

  def validateNotNull(spark: SparkSession, df: DataFrame, primary_key_col_list: List[String]): List[DataFrame] = {
    var primary_correct_col = ""
    var primary_incorrect_col = ""

    for (z <- primary_key_col_list) {
      primary_correct_col = primary_correct_col + "and length(trim(" + z + "))>0 and  trim(" + z + ")<>'(null)' and trim(" + z + ") not like '%?'"
      primary_incorrect_col = primary_incorrect_col + "OR " + z + " is null OR length(trim(" + z + "))=0  OR trim(" + z + ")='(null)' OR trim(" + z + ") like '%?'"
    }
    df.show
    df.registerTempTable("null_data")
    val valid_select_query = "select * from null_data where " + (primary_correct_col.drop(3))
    val invalid_select_query = "select * from null_data where " + (primary_incorrect_col.drop(2))

    val validDF = spark.sql(valid_select_query)
    val invalidDF = spark.sql(invalid_select_query)
    List(validDF, invalidDF)
  }

  /**
   * validateColumnLength function checks length of a column.
   *  For valid record it will store data in valid record
   * and for invalid records it will create two additional columns "err_desc " and "err_col_name" to store information about error and union invalid records for other columns error.
   *
   * @param inputDF			: Input Dataframe from HIVE table
   * @param validDF			: Valid records which passed the validateNotNull Check
   * @param schemaList	: List of Hive table columns read from YAML file on which validation is to be performed
   * @return List[DataFrame] : List of dataframe of valid and invalid records.
   */
  def validateColumnLength(inputDF: DataFrame, validDF: DataFrame, schemaList: Array[String]): List[DataFrame] = {
    var invalidRecords: DataFrame = (inputDF.withColumn("err_desc", lit(""))).withColumn("err_col_name", lit("")) limit (0)
    var validRecords: DataFrame = validDF
    var invalidRecordColumnWise: DataFrame = null
    try {
      for (columnDetails <- schemaList) {
        var columnName = inputDF { columnDetails.split('|')(0) }
        if ((columnDetails.split('|')(1)).contains(",")) {
          validRecords = validRecords.filter(length(columnName) <= (columnDetails.split('|')(1).split(',')(0).toInt + 1))
          invalidRecordColumnWise = (inputDF.filter(length(columnName) > (columnDetails.split('|')(1).split(',')(0).toInt + 1)).withColumn("err_desc", lit("Length check failed"))).withColumn("err_col_name", lit(columnName.toString()))
        } else {
          validRecords = validRecords.filter(length(columnName) <= (columnDetails.split('|')(1)))
          invalidRecordColumnWise = (inputDF.filter(length(columnName) > (columnDetails.split('|')(1))).withColumn("err_desc", lit("Length check failed"))).withColumn("err_col_name", lit(columnName.toString()))
        }
        invalidRecords = invalidRecords.unionAll(invalidRecordColumnWise)
        invalidRecordColumnWise = null
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to execute SQL please check yarn logs with applicationId " + SparkEnv.get.conf.getAppId)
        println(e.printStackTrace())
        throw e
    }
    return List(validRecords, invalidRecords)
  }

  def IncrementalType2NonMD5(
    incremental_df:       DataFrame,
    target_df:            DataFrame,
    primary_key_col_list: Array[String],
    partition_col:        String,
    start_date:           String,
    end_date:             String)(implicit spark: SparkSession): DataFrame = {
    try {
      logger.info("Starting CDC")
      target_df.createOrReplaceTempView("TGT_DF")
      incremental_df.createOrReplaceTempView("INC_DF")
      val history_df = if (partition_col != "") spark.sql("select * from TGT_DF where " + partition_col + " in (select distinct " + partition_col + " from INC_DF)") else target_df
      logger.info("Starting CDC")
      val dataSchema = history_df.columns
      val dfInnerJoin = history_df.filter(col("dl_load_flag")
        .eqNullSafe("Y")).as("L1").join(broadcast(incremental_df), primary_key_col_list)
        .select("L1.*").select(dataSchema.head, dataSchema.tail: _*)
      history_df.printSchema
      dfInnerJoin.printSchema
      val unchangedData = history_df.except(dfInnerJoin)
      if ((null != start_date) && (null != end_date)) {
        logger.info("Using custom start_date and end_date")
        val changedData = dfInnerJoin.drop("dl_load_flag", "end_date").withColumn("dl_load_flag", lit("N")).withColumn("end_date", lit(end_date)).select(dataSchema.head, dataSchema.tail: _*)
        val finalData = unchangedData.union(incremental_df.withColumn("dl_load_flag", lit("Y")).withColumn("dl_load_flag", lit("Y")).withColumn("start_date", lit(start_date)).withColumn("end_date", lit(end_date)).select(dataSchema.head, dataSchema.tail: _*))
          .union(changedData)
        logger.info("Completed CDC for Incremental Feed Type 2 Non-MD5 !!!")
        finalData
      } else {
        println("In else")
        val changedData = dfInnerJoin.drop("dl_load_flag", "end_date").withColumn("dl_load_flag", lit("N")).withColumn("end_date", lit(getCurrentTimestamp)).select(dataSchema.head, dataSchema.tail: _*)
        val finalData = unchangedData.union(incremental_df.withColumn("dl_load_flag", lit("Y")).withColumn("dl_load_flag", lit("Y")).withColumn("start_date", lit(getCurrentTimestamp)).withColumn("end_date", lit("9999-12-31 00:00:00")).select(dataSchema.head, dataSchema.tail: _*))
          .union(changedData)
        logger.info("Completed CDC for Incremental Feed Type 2 Non-MD5 !!!")
        finalData
      }
    } catch {
      case e: Exception =>
        logger.error("Please check yarn logs for exact error with applicationId " + SparkEnv.get.conf.getAppId)
        println(e.printStackTrace())
        throw e
        return null
    }
  }

}