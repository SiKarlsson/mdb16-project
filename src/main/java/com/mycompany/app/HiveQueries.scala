package com.mycompany.app

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
// OBS run as hduser, need classpaths 
object HiveQueries{
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("Spark Queries").setMaster("local"))
		val context = new HiveContext(sc)
		import context.implicits._
		import context.sql
		//context.setConf("hive.metastore.warehouse.dir", "hdfs://http://localhost:50070/user/hive/warehouse")
		//context.sql("SET hive.metastore.warehouse.dir=hdfs://localhost:50070/user/hive/warehouse");
		val res = context.sql("SELECT * FROM artisttt WHERE type='1' AND begindate LIKE('199%')")
		//println("----------------här------------------")
		res.show()

}
}