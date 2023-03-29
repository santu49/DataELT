package org.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.DriverManager
import java.io.File
import java.util.Properties

object dataProjectMain {

  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataProject")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR");
    return spark;
  }

  def checkConfigFile(Json_file_path: String, file_type: String): Any = {
    try {
      val file = new File(Json_file_path)
      if (file.exists() && file.isFile()) {
        return "Success";
      }
      else {
        // File does not exist or is not a regular file
        throw new Exception("File does not exist")
      }
    } catch {
      case e: Exception => {
        println("An error occurred: " + e.getMessage);
      }
    }
  }


  def readConfigFile(Json_file_path: String, file_type: String): DataFrame = {
    val spark = createSparkSession();
    val Json_fileData = spark.read.format(file_type).option("multiline", true).load(Json_file_path);
    return Json_fileData;
  }


  def getSrcConnections(configFileData: DataFrame): DataFrame = {
    val src_ct_df = configFileData.filter(configFileData("type") === "source").select("connectionType");
    return src_ct_df;
  }

  def getTarConnections(configFileData: DataFrame): DataFrame = {
    val tar_ct_df = configFileData.filter(configFileData("type") === "target").select("connectionType");
    return tar_ct_df;
  }

  def getDataFromFile(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("filePath", "fileType");
    import spark.implicits._
    val src_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
    val src_file_data = spark.read.format(src_file_type).option("header", "true").load(src_file_path);
    return src_file_data;
  }
  def getDataFromPostGre(configFileData: DataFrame): DataFrame = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "source").select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    //    val pgConnectionType = new Properties();
    //    pgConnectionType.setProperty("user", s"$post_userName");
    //    pgConnectionType.setProperty("password", s"$post_password");
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
    val src_ct_df = spark.read
      .format("jdbc")
      .option("url", s"$url")
      .option("dbtable", s"$tableUrl")
      .option("user", s"$post_userName")
      .option("password", s"$post_password")
      .option("driver", "org.postgresql.Driver")
      .load()
//    src_ct_df.show(10);
    return src_ct_df;
  }

  def putDataInPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val pgConnectionType = new Properties();
    pgConnectionType.setProperty("user", s"$post_userName");
    pgConnectionType.setProperty("password", s"$post_password");
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name"
    val url = s"jdbc:postgresql://localhost:5432/$post_databaseName"
    src_file_data.write
      .mode(SaveMode.Append)
      .jdbc(url, s"$tableUrl", pgConnectionType)

    return "Successfully load data";
  }

  def putDataInLocalFile(configFileData:DataFrame,srcFileData:DataFrame):String={
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target").select("filePath", "fileType");
    import spark.implicits._
    val tar_file_path = df.select("filePath").distinct().map(f => f.getString(0)).collect().toList(0);
    val tar_file_type = df.select("fileType").distinct().map(f => f.getString(0)).collect().toList(0);
//    val src_file_data = spark.read.format(src_file_type).option("header", "true").load(src_file_path);
//    srcFileData.show()
    srcFileData.coalesce(1).write.format(tar_file_type).option("header","true").save(tar_file_path);

    return "Successfully load data"

  }

  def putDataInAWSPostgreSQL(configFileData: DataFrame, src_file_data: DataFrame): String = {
    val spark = createSparkSession();
    val df = configFileData.filter(configFileData("type") === "target")
      .select("userName", "password", "dataBaseName", "schemaName", "tableName");
    import spark.implicits._
    val post_userName = df.select("userName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_password = df.select("password").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_databaseName = df.select("dataBaseName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_Schema_Name = df.select("schemaName").distinct().map(f => f.getString(0)).collect().toList(0);
    val post_table_name = df.select("tableName").distinct().map(f => f.getString(0)).collect().toList(0);

    val pgConnectionType = new Properties();
    pgConnectionType.setProperty("user", s"$post_userName");
    pgConnectionType.setProperty("password", s"$post_password");

    val url = "jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/";
    val tableUrl = s"\"$post_Schema_Name\".$post_table_name";
    val conn = DriverManager.getConnection(url, post_userName, post_password);
    val statement = conn.createStatement();
    statement.executeUpdate(s"CREATE DATABASE $post_databaseName")
    val url1 = s"jdbc:postgresql://database-1.ckaqa1w4hirj.ap-south-1.rds.amazonaws.com:5432/$post_databaseName"
    val conn1 = DriverManager.getConnection(url1, post_userName, post_password)
    val statement1 = conn1.createStatement()
    statement1.executeUpdate(s"CREATE SCHEMA $post_Schema_Name");
    // write file to destination
    src_file_data.write
      .mode(SaveMode.Overwrite)
      .jdbc(url1, s"$tableUrl", pgConnectionType)

    return "Successfully load data from local file to AWS PostgreSql";
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession();
    val config_Json_file_path = "src/main/resources/configFile.json";
    val file_type = "json";
    // checking file is there or not
    val check = checkConfigFile(config_Json_file_path, file_type);
    var configFileData = spark.emptyDataFrame;
    if (check == "Success") {
      configFileData = readConfigFile(config_Json_file_path, file_type);
      // configFileData
      //configFileData.show();
      val srcConnectionData = getSrcConnections(configFileData);
      val tarConnectionData = getTarConnections(configFileData);
//      srcConnectuonData.show();
      //tarConnectuonData.show();
      import spark.implicits._
      val srcConnectionTypeData = srcConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      val tarConnectionTypeData = tarConnectionData.select("connectionType").distinct()
        .map(f => f.getString(0)).collect().toList;
      // println(tarConectionTypeData(0));
      // creating empty dataframe
      var srcFileData = spark.emptyDataFrame
//      checking the source
      if (srcConnectionTypeData(0) == "fileSystem") {
        srcFileData = getDataFromFile(configFileData);
        //          srcFileData.show(10);
        //          write here
      } else if (srcConnectionTypeData(0) == "postgreSQL") {
        srcFileData = getDataFromPostGre(configFileData);
//        srcFileData.show(10);
      }


//      checking the target
      if (tarConnectionTypeData(0) == "postgreSQL") {
        val message = putDataInPostgreSQL(configFileData, srcFileData);
        println(message);
      }else if(tarConnectionTypeData(0)=="fileSystem"){
        val message = putDataInLocalFile(configFileData, srcFileData);
        println(message);
      }else if(tarConnectionTypeData(0)=="AwsPostgreSQL"){
        val message = putDataInAWSPostgreSQL(configFileData, srcFileData);
        println(message);

      }
    }
  }
}