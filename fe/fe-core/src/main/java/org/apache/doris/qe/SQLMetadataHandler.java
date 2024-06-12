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

package org.apache.doris.qe;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SQLMetadataHandler {

    private static final Pattern METADATA_PATTERN = Pattern.compile("/\\*OLAP:([^*]*)\\*/");

    /**
     * 将键值对元数据添加到 SQL 语句的注释部分。
     * @param sql 原始 SQL 语句
     * @param metadata 键值对元数据
     * @return 带有元数据注释的 SQL 语句
     */
    public static String wrapSQL(String sql, Map<String, String> metadata) {
        StringBuilder wrappedSQL = new StringBuilder("/*OLAP:");
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            wrappedSQL.append(entry.getKey()).append("=").append(entry.getValue()).append(";");
        }
        wrappedSQL.append("*/ ").append(sql);
        return wrappedSQL.toString();
    }

    /**
     * 从带有注释的 SQL 语句中提取键值对元数据。
     * @param sql 带有元数据注释的 SQL 语句
     * @return 提取出的键值对元数据
     */
    public static Map<String, String> parseMetadata(String sql) {
        Map<String, String> metadata = new HashMap<>();
        Matcher matcher = METADATA_PATTERN.matcher(sql);
        if (matcher.find()) {
            String metadataString = matcher.group(1);
            String[] keyValuePairs = metadataString.split(";");
            for (String keyValuePair : keyValuePairs) {
                if (!keyValuePair.trim().isEmpty()) {
                    String[] parts = keyValuePair.split("=");
                    if (parts.length == 2) {
                        metadata.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
        }
        return metadata;
    }

    /**
     * 从 SQL 语句中移除注释部分，返回干净的 SQL 语句。
     * @param sql 带有元数据注释的 SQL 语句
     * @return 干净的 SQL 语句
     */
    public static String removeMetadata(String sql) {
        return sql.replaceAll(METADATA_PATTERN.pattern(), "").trim();
    }

    public static void main(String[] args) {
        String sql = "SELECT * FROM users WHERE id = 1";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("user", "admin");
        metadata.put("transaction", "12345");

        // 封装SQL
        String wrappedSQL = SQLMetadataHandler.wrapSQL(sql, metadata);
        System.out.println("Wrapped SQL: " + wrappedSQL);

        // 解析SQL
        Map<String, String> parsedMetadata = SQLMetadataHandler.parseMetadata(wrappedSQL);
        System.out.println("Parsed Metadata: " + parsedMetadata);

        // 移除元数据
        String cleanSQL = SQLMetadataHandler.removeMetadata(wrappedSQL);
        System.out.println("Clean SQL: " + cleanSQL);
    }
}
