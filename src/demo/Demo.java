package demo;

import java.io.IOException;

import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import models.SuccessEvent;

public class Demo {
	@SuppressWarnings("unused")
	public static void main(String[] args) throws TimeoutException, StreamingQueryException, ParseException, IOException {
		try (SparkSession spark = SparkSession.builder().appName("Read kafka").getOrCreate()) {
			Dataset<String> data = spark
					.readStream()
			        .format("kafka")
			        .option("kafka.bootstrap.servers", "localhost:9092")
			        .option("subscribe", "tiki-2")
			        .load()
					.selectExpr("CAST(value AS STRING)")
					.as(Encoders.STRING());
			
			Encoder<SuccessEvent> successEventEncoder = Encoders.bean(SuccessEvent.class);
			
			Dataset<SuccessEvent> dataSuccess = data.map(new MapFunction<String, SuccessEvent>() {
				private static final long serialVersionUID = 1L;
				@Override
				public SuccessEvent call(String value) throws Exception {
					JSONParser parse = new JSONParser();
					JSONObject json = (JSONObject) parse.parse(value);
					
					long guid = (long) json.get("guid");
					long time = (long) json.get("time");
					String url = (String) json.get("url");
					
					SuccessEvent success = new SuccessEvent(time, url, guid);
					return success;
				}
			}, successEventEncoder);
			
			Dataset<Row> dataSuccessRow = dataSuccess.toDF();
			
			Dataset<Row> dataView = spark
					.readStream()
					.schema(new StructType()
							.add("time_create", "long")
							.add("cookie_create", "long")
							.add("browser_code", "integer")
							.add("browser_ver", "string")
							.add("os_code", "integer")
							.add("os_ver", "string")
							.add("ip", "long")
							.add("guid", "long")
							.add("domain", "string")
							.add("path", "string")
							.add("geo", "integer")
							.add("locid", "integer")
							.add("flashver", "string")
							.add("jre", "string")
							.add("siteid", "integer")
							.add("channelid", "integer")
							.add("refer", "string")
							.add("sr", "string")
							.add("sc", "string")
							.add("fullrefer", "string")
							.add("requestid", "long")
							.add("fp_guid", "string")
							.add("category", "string")
							.add("googleId", "string")
							.add("tabActive", "string"))
					.parquet("/home/trannguyenhan/dataset/data/demo_view");
					
			dataView =  dataView.filter(dataView.col("domain").contains("tiki"));
			
			dataView.createOrReplaceTempView("dataView");
			dataSuccessRow.createOrReplaceTempView("dataSuccess");
			spark.sql("select dataSuccess.guid, dataSuccess.url, dataView.time_create, dataSuccess.timeCreate"
					+ " from dataView inner join dataSuccess on dataView.guid = dataSuccess.guid").createOrReplaceTempView("dataJoin");
			
			Dataset<Row> dataResult = spark.sql("select * from dataJoin");
			
			dataResult.printSchema();
			StreamingQuery query = dataResult.writeStream()
					.trigger(Trigger.ProcessingTime(10000))
					.outputMode("append")
					.format("console")
					.start();			
			
			query.awaitTermination();
			
			
		}
	}
	
	
}
