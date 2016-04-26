package com.mycompany.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
//import org.apache.spark.sql.types.DataTypes 
/*
//import org.apache.spark._
import org.apache.spark.SparkContext

//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.api.java._
import org.apache.spark.sql.Row;
import org.apache.spark.sql._


*/

//import org.apache.spark.sql.api.java.types;
object SparkQueries {
	def main(args: Array[String]) {

		 //types.{StructType, StructField, StringgType}
		
		val pathToFile = "/user/hduser/artist"
		val conf = new SparkConf().setMaster("local")
		val sc = new SparkContext(conf)
		//val sc = new SparkContext(new SparkConf().setAppName("Spark Queries").setMaster("local"))
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		// get file from hdfs
		val artist = sc.textFile(pathToFile)
		val schemaString = "id name gid begindate enddate type"
		//import org.apache.spark.sql.types._//{StructType, StructField, StringType};
		// Import Row.
		import org.apache.spark.sql.Row;

		// Import Spark SQL data types
		import org.apache.spark.sql.types.{StructType,StructField,StringType};
		println("HERE 0")
		val part1 = schemaString.split(" ")
		println("HERE 1")
		val part2 = part1.map(fieldName => StructField(fieldName, StringType, true))
		println("HERE 2")
		//val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
		val schema = StructType(part2)
		println("HERE 3")
		// extract columns from file (split by ',')
		val rowRDD = artist.map(_.split(",")).map(a => Row(a(0).trim,a(1).trim, a(2).trim, a(7).trim, a(8).trim, a(9).trim))
		val artistDF = sqlContext.createDataFrame(rowRDD, schema)
		
		// tmptable is created
		artistDF.registerTempTable("artistt")
		val allrecords = sqlContext.sql("SELECT * FROM artistt")
		allrecords.show()
		val bornInThe90s = sqlContext.sql("SELECT * FROM artistt WHERE type='1' AND begindate LIKE('199%')")
		bornInThe90s.show()
		
	}
}