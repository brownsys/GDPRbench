/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for dumping to a trace file.
 */
public class TraceFileClient extends DB {
  /** Name of database (and DB file). */
  public static final String FILE_PATH_PROPERTY = "file.path";
  public static final String APPEND_PROPERTY = "file.append";
  public static final String SHARDING_PROPERTY = "file.shard";
  /** Table schema configuration. */
  public static final String TABLE_NAME = "usertable";
  public static final String PRIMARY_KEY = "YCSB_KEY";
  public static final String METADATA_COLUMN = "PUR";

  /** SQL for table creation. */
  public static final String CREATE_TABLE_SQL_NONSHARDING =
      "CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY,"
      + " DEC VARCHAR, USR VARCHAR, SRC VARCHAR, OBJ VARCHAR,"
      + " CAT VARCHAR, ACL VARCHAR, Data VARCHAR, PUR VARCHAR,"
      + " SHR VARCHAR, TTL VARCHAR);"
      + "\n"
      + "CREATE INDEX if not exists pur_index ON usertable(PUR);\n";

  public static final String CREATE_TABLE_SQL_SHARDING =
      "SET echo;"
      + "\n"
      + "CREATE TABLE main(ID VARCHAR PRIMARY KEY, PII_attr VARCHAR);\n"
      + "CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY,"
      + " DEC VARCHAR, USR VARCHAR, SRC VARCHAR, OBJ VARCHAR,"
      + " CAT VARCHAR, ACL VARCHAR, Data VARCHAR, PUR VARCHAR,"
      + " SHR VARCHAR, TTL VARCHAR, FOREIGN KEY(PUR) REFERENCES main(ID));"
      + "\n"
      + "INSERT INTO main VALUES ('PUR=ads++++++++++++++++++++++++++++++++++++++++++++"
      + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'ads');\n"
      + "INSERT INTO main VALUES ('PUR=2fa++++++++++++++++++++++++++++++++++++++++++++"
      + "+++++++++++++++++++++++++++++++++++++++++++++++++', '2fa');\n"
      + "INSERT INTO main VALUES ('PUR=msg++++++++++++++++++++++++++++++++++++++++++++"
      + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'msg');\n"
      + "INSERT INTO main VALUES ('PUR=backup+++++++++++++++++++++++++++++++++++++++++"
      + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'backup');";
  
  public static final List<String> COLUMNS = Arrays.asList(
      new String[] {"DEC", "USR", "SRC", "OBJ", "CAT", "ACL", "Data", "PUR", "SHR", "TTL"});

  private FileWriter file;
  private PrintWriter writer;
  private boolean append;
  private boolean sharding;

  public void init() throws DBException {
    Properties props = getProperties();
    String filePath = props.getProperty(FILE_PATH_PROPERTY);
    String fileAppend = props.getProperty(APPEND_PROPERTY, "no");
    String shardingStr = props.getProperty(SHARDING_PROPERTY, "no");
    
    this.append = fileAppend.equalsIgnoreCase("yes");
    this.sharding = shardingStr.equalsIgnoreCase("yes");
    
    try {
      this.file = new FileWriter(filePath, this.append);
      this.writer = new PrintWriter(this.file);
    } catch (IOException io) {
      throw new DBException("IO exception raised opening file", io);
    }
    
    if (!this.append) {
      if (this.sharding) { 
        this.writer.println(CREATE_TABLE_SQL_SHARDING);
      } else {
        this.writer.println(CREATE_TABLE_SQL_NONSHARDING);
      }
    } else {
      this.writer.println("# Start of benchmark");
    }
  }

  public void cleanup() throws DBException {
    try {
      this.writer.close();
      this.file.close();
    } catch (IOException io) {
      throw new DBException("IO exception raised closing file", io);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("='");
    builder.append(key);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status readMeta(String table, String cond, String keymatch,
      Vector<HashMap<String, ByteIterator>> result) {
    if (!keymatch.equals("user*")) {
      System.out.println("Update meta error " + keymatch);
      return Status.ERROR;
    }
    if (!cond.startsWith(METADATA_COLUMN)) {
      System.out.println("Update meta condition error " + cond);
      return Status.ERROR;
    }
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(METADATA_COLUMN);
    builder.append("='");
    builder.append(cond);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(table);
    builder.append(" VALUES('");
    builder.append(key);
    builder.append("'");
    for (String col : COLUMNS) {
      builder.append(",'");
      builder.append(values.get(col).toString());
      builder.append("'");
    }
    builder.append(");");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status insertTTL(String table, String key, Map<String, ByteIterator> values, int ttl) {
    return this.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(table);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("='");
    builder.append(key);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status deleteMeta(String table, String condition, String keymatch) {
    if (!keymatch.equals("user*")) {
      System.out.println("Update meta error " + keymatch);
      return Status.ERROR;
    }
    if (!condition.startsWith(METADATA_COLUMN)) {
      System.out.println("Update meta condition error " + condition);
      return Status.ERROR;
    }
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(table);
    builder.append(" WHERE ");
    builder.append(METADATA_COLUMN);
    builder.append("='");
    builder.append(condition);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder builder = new StringBuilder("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      builder.append(e.getKey());
      builder.append("='");
      builder.append(e.getValue());
      builder.append("',");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("='");
    builder.append(key);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status updateMeta(String table, String condition, String keymatch, String fieldname,
      String metadatavalue) {
    if (!keymatch.equals("user*")) {
      System.out.println("Update meta error " + keymatch);
      return Status.ERROR;
    }
    if (!condition.startsWith(METADATA_COLUMN)) {
      System.out.println("Update meta condition error " + condition);
      return Status.ERROR;
    }
    StringBuilder builder = new StringBuilder("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.append(fieldname);
    builder.append("='");
    builder.append(metadatavalue);
    builder.append("' WHERE ");
    builder.append(METADATA_COLUMN);
    builder.append("='");
    builder.append(condition);
    builder.append("';");
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append(">='");
    builder.append(startkey);
    builder.append("' ORDER BY ");
    builder.append(PRIMARY_KEY);
    builder.append(" LIMIT ");
    builder.append(recordcount);
    this.writer.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status verifyTTL(String table, long recordcount) {
    return Status.OK;
  }

  @Override
  public Status readLog(String table, int logcount) {
    return Status.OK;
  }
}
