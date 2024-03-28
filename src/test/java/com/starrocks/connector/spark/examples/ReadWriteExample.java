package com.starrocks.connector.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadWriteExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config(new SparkConf())
                .master("local[1]")
                .appName("read_example")
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS tb_primary_key\n" +
                "       USING starrocks\n" +
                "       OPTIONS\n" +
                "       (\n" +
                "           'starrocks.table.identifier' = 'test.tb_primary_key',\n" +
                "           'starrocks.fe.http.url' = '10.37.80.244:8031',\n" +
                "           'starrocks.fe.jdbc.url' = 'jdbc:mysql://10.37.80.244:9031',\n" +
                "           'starrocks.user' = 'root',\n" +
                "           'starrocks.password' = ''\n" +
                "       );");

        // Write Data
        String insertSql = "INSERT INTO tb_primary_key VALUES (1002, 'bytedance.com', 5)";
        spark.sql(insertSql);
        System.out.println(insertSql);

        // Read Data
        String selectSql = "SELECT * FROM tb_primary_key WHERE user_id > 1001";
        Dataset<Row> df = spark.sql(selectSql);
        System.out.println(selectSql);
        df.show();

        spark.stop();
    }

}
