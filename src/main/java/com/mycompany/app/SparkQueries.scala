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

	def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000.0 + "ms")
    result
}

	def main(args: Array[String]) {
		
		
		val artistPath = "hdfs://localhost:54310/user/hduser/artist"
		val trackPath = "hdfs://localhost:54310/user/hduser/track"
		val track_tagPath = "hdfs://localhost:54310/user/hduser/track_tag"

		val sc = new SparkContext(new SparkConf().setAppName("Spark Queries").setMaster("local"))
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		
		// ======================================
		// ARTIST TABLE:
		// get file from hdfs
		val artist = sc.textFile(artistPath)
		val artistSchemaString = "id name gid begindate enddate type"
		import org.apache.spark.sql.Row;

		// Import Spark SQL data types
		import org.apache.spark.sql.types.{StructType,StructField,StringType};
		val artistSchema = StructType(artistSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
		// extract columns from file (split by ',')
		val artistRowRDD = artist.map(_.split(",")).map(a => Row(a(0).trim,a(1).trim, a(2).trim, a(7).trim, a(8).trim, a(9).trim))
		val artistDF = sqlContext.createDataFrame(artistRowRDD, artistSchema)
		
		// tmptable is created
		artistDF.registerTempTable("artistt")

		// ======================================
		// TRACK TABLE:
		// get file from hdfs
		val track = sc.textFile(trackPath)
		val trackSchemaString = "id artist name gid length year modpending"
		import org.apache.spark.sql.Row;

		// Import Spark SQL data types
		import org.apache.spark.sql.types.{StructType,StructField,StringType};
		val trackSchema = StructType(trackSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
		// extract columns from file (split by ',')
		val trackRowRDD = track.map(_.split(",")).map(a => Row(a(0).trim,a(1).trim, a(2).trim, a(3).trim, a(4).trim, a(5).trim, a(6).trim))
		val trackDF = sqlContext.createDataFrame(trackRowRDD, trackSchema)
		
		// tmptable is created
		trackDF.registerTempTable("trackk")

		// ======================================
		// TRACK_TAG:
		// get file from hdfs
		val track_tag = sc.textFile(track_tagPath)
		val track_tagSchemaString = "track tag count"
		import org.apache.spark.sql.Row;

		// Import Spark SQL data types
		import org.apache.spark.sql.types.{StructType,StructField,StringType};
		val track_tagSchema = StructType(track_tagSchemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
		// extract columns from file (split by ',')
		val track_tagRowRDD = track_tag.map(_.split(",")).map(a => Row(a(0).trim,a(1).trim, a(2).trim))
		val track_tagDF = sqlContext.createDataFrame(track_tagRowRDD, track_tagSchema)
		
		// tmptable is created
		track_tagDF.registerTempTable("track_tagg")

		// TESTS:
		// Count the tuples in each table.
		/*
		val artistCount = time{sqlContext.sql("SELECT * FROM artistt").count()}
		val trackCount = time{sqlContext.sql("SELECT * FROM trackk").count()}
		val track_tagCount = time{sqlContext.sql("SELECT * FROM track_tagg").count()}
		println("artist: " + artistCount)
		println("track: " + trackCount)
		println("track_tag: " + track_tagCount)
		*/

		println("================== My outputs ==================")
		// testing joins
		val joins = time { sqlContext.sql("SELECT artistt.name, trackk.name, track_tagg.tag FROM trackk,artistt,track_tagg WHERE artistt.id = trackk.artist AND trackk.id = track_tagg.track").count() }
		println(joins)
		
		// testing GROUP BY
		val sumGroupBy = time{sqlContext.sql("SELECT trackk.name, SUM(cast(length AS INT)) FROM trackk GROUP BY trackk.name HAVING COUNT (trackk.name) > 1").count()}
		println(sumGroupBy)
		println("================== END ==================")
		
		
	}
}