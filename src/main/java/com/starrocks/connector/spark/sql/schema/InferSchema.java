// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql.schema;

import com.starrocks.connector.spark.exception.StarrocksException;
import com.starrocks.connector.spark.sql.conf.SimpleStarRocksConfig;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.connect.StarRocksConnector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class InferSchema {

    public static StructType inferSchema(Map<String, String> options) {
        SimpleStarRocksConfig config = new SimpleStarRocksConfig(options);
        // 获取目标库表的 Column 信息
        StarRocksSchema starocksSchema = StarRocksConnector.getSchema(config);
        return inferSchema(starocksSchema, config);
    }

    /**
     * 将目标 SR 库表的 Column 信息转换封装成 Spark StructType 对象
     */
    public static StructType inferSchema(StarRocksSchema starRocksSchema, StarRocksConfig config) {
        String[] inputColumns = config.getColumns();
        List<StarRocksField> starRocksFields;
        if (inputColumns == null || inputColumns.length == 0) {
            starRocksFields = starRocksSchema.getColumns();
        }
        // 在配置中指定了 column，需要校验这些 column 是否存在
        else {
            starRocksFields = new ArrayList<>();
            List<String> nonExistedColumns = new ArrayList<>();
            for (String column : inputColumns) {
                StarRocksField field = starRocksSchema.getField(column);
                if (field == null) {
                    nonExistedColumns.add(column);
                }
                starRocksFields.add(field);
            }
            if (!nonExistedColumns.isEmpty()) {
                throw new StarrocksException(
                        String.format("Can't find those columns %s in StarRocks table `%s`.`%s`. " +
                                "Please check your configuration 'starrocks.columns' to make sure all columns exist in the table",
                                nonExistedColumns, config.getDatabase(), config.getTable()));
            }
        }

        Map<String, StructField> customTypes = parseCustomTypes(config.getColumnTypes());
        // 将 SR Field 转换成 Spark Field
        List<StructField> fields = new ArrayList<>();
        for (StarRocksField field : starRocksFields) {
            if (customTypes.containsKey(field.getName())) {
                fields.add(customTypes.get(field.getName()));
            } else {
                fields.add(inferStructField(field));
            }
        }

        // 封装 Filed 为 StructType
        return DataTypes.createStructType(fields);
    }

    static Map<String, StructField> parseCustomTypes(String columnTypes) {
        if (columnTypes == null) {
            return new HashMap<>(0);
        }

        Map<String, StructField> customTypes = new HashMap<>();
        StructType customSchema = StructType.fromDDL(columnTypes);
        for (StructField field : customSchema.fields()) {
            customTypes.put(field.name(), field);
        }
        return customTypes;
    }

    static StructField inferStructField(StarRocksField field) {
        // 映射 SR 类型到 Spark 类型
        DataType dataType = inferDataType(field);
        return new StructField(field.getName(), dataType, true, Metadata.empty());
    }

    /**
     * 映射 SR 类型到 Spark 类型
     */
    static DataType inferDataType(StarRocksField field) {
        String type = field.getType().toLowerCase(Locale.ROOT);
        switch (type) {
            case "tinyint":
                // mysql does not have boolean type, and starrocks `information_schema`.`COLUMNS` will return
                // a "tinyint" data type for both StarRocks BOOLEAN and TINYINT type, We distinguish them by
                // column size, and the size of BOOLEAN is null
                return field.getSize() == null ? DataTypes.BooleanType : DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "int":
                return DataTypes.IntegerType;
            case "bigint":
                return DataTypes.LongType;
            case "bigint unsigned":
                return DataTypes.StringType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
                return DataTypes.createDecimalType(Integer.parseInt(field.getSize()), Integer.parseInt(field.getScale()));
            case "char":
            case "varchar":
            case "json":
                return DataTypes.StringType;
            case "date":
                return DataTypes.DateType;
            case "datetime":
                return DataTypes.TimestampType;
            default:
                throw new UnsupportedOperationException(String.format(
                        "Unsupported starrocks type, column name: %s, data type: %s", field.getName(), field.getType()));
        }
    }
}
