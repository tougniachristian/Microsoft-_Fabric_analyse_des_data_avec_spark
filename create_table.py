df.write.format("delta").saveAsTable("salesorders")
spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)