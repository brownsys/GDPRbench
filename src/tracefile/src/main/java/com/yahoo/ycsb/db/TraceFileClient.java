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
  public static final int NUM_USERS = 1000;

  /** Name of database (and DB file). */
  public static final String SHARDED_FILE_PATH_PROPERTY = "sharded.path";
  public static final String UNSHARDED_FILE_PATH_PROPERTY = "unsharded.path";
  public static final String APPEND_PROPERTY = "file.append";
  /** Table schema configuration. */
  public static final String TABLE_NAME = "usertable";
  public static final String PRIMARY_KEY = "YCSB_KEY";
  public static final String SCAN_VIEW = "sview";
  public static final String PUR_VIEW = "pview";

  /** SQL for table creation. */
  public static final String UNSHARDED_SCHEMA =
      "CREATE TABLE usertable(YCSB_KEY VARCHAR(100) PRIMARY KEY,"
      + " `DEC` VARCHAR(100), USR VARCHAR(100), SRC VARCHAR(100), OBJ VARCHAR(100),"
      + " CAT VARCHAR(100), ACL VARCHAR(100), Data VARCHAR(100), PUR VARCHAR(100),"
      + " SHR VARCHAR(100), TTL VARCHAR(100)) ENGINE ROCKSDB;\n"
      + "CREATE INDEX usr_index ON usertable(USR);\n"
      + "CREATE INDEX pur_index ON usertable(PUR);\n";

  public static final String SHARDED_SCHEMA =
      "CREATE TABLE main(PII_usr VARCHAR(100) PRIMARY KEY);\n"
      + "CREATE TABLE usertable(YCSB_KEY VARCHAR(100) PRIMARY KEY,"
      + " `DEC` VARCHAR(100), USR VARCHAR(100), SRC VARCHAR(100), OBJ VARCHAR(100),"
      + " CAT VARCHAR(100), ACL VARCHAR(100), Data VARCHAR(100), PUR VARCHAR(100),"
      + " SHR VARCHAR(100), TTL VARCHAR(100), FOREIGN KEY(USR) REFERENCES main(ID));\n"
      + "CREATE INDEX pk_index ON usertable(YCSB_KEY);\n"
      + "CREATE VIEW sview AS '\"SELECT * FROM usertable ORDER BY YCSB_KEY LIMIT ?\"';\n"
      + "CREATE VIEW pview AS '\"SELECT * FROM usertable WHERE PUR = ?\"';";
  
  public static final List<String> COLUMNS = Arrays.asList(
      new String[] {"DEC", "USR", "SRC", "OBJ", "CAT", "ACL", "Data", "PUR", "SHR", "TTL"});

  public static boolean isUsr(int fieldnum) {
    assert fieldnum == 0 || fieldnum == 2;  // 0 is PUR.
    return fieldnum == 2;
  }

  public static String escapeColumn(String colname) {
    if (colname.equalsIgnoreCase("DEC")) {
      return "`DEC`";
    }
    return colname;
  }

  /* Files and writers for sharded and unsharded tracefile */
  private FileWriter sfile;
  private PrintWriter swriter;
  private FileWriter ufile;
  private PrintWriter uwriter;
  private boolean append;

  public void init() throws DBException {
    Properties props = getProperties();
    String sfilePath = props.getProperty(SHARDED_FILE_PATH_PROPERTY);
    String ufilePath = props.getProperty(UNSHARDED_FILE_PATH_PROPERTY);
    String fileAppend = props.getProperty(APPEND_PROPERTY, "no");

    try {
      this.append = fileAppend.equalsIgnoreCase("yes");
      this.sfile = new FileWriter(sfilePath, this.append);
      this.swriter = new PrintWriter(this.sfile);
      this.ufile = new FileWriter(ufilePath, this.append);
      this.uwriter = new PrintWriter(this.ufile);
    } catch (IOException io) {
      throw new DBException("IO exception raised opening file", io);
    }
    
    if (!this.append) {
      // Sharded schema creation.
      this.swriter.println(SHARDED_SCHEMA);
      // Unsharded schema creation.
      this.uwriter.println(UNSHARDED_SCHEMA);
    } else {
      this.swriter.println("# Start of benchmark");
      this.swriter.println("# perf start");
      this.uwriter.println("# Start of benchmark");
      this.uwriter.println("# perf start");
    }
  }

  public void cleanup() throws DBException {
    try {
      this.swriter.close();
      this.sfile.close();
      this.uwriter.close();
      this.ufile.close();
    } catch (IOException io) {
      throw new DBException("IO exception raised closing file", io);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    assert fields.containsAll(COLUMNS) && COLUMNS.containsAll(fields);
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append(" = '");
    builder.append(key);
    builder.append("';");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status readMeta(String table, int fieldnum, String cond, String keymatch,
      Vector<HashMap<String, ByteIterator>> result) {
    if (!keymatch.equals("key*")) {
      System.out.println("Read meta error " + keymatch);
      return Status.ERROR;
    }
    if (isUsr(fieldnum)) {
      StringBuilder builder = new StringBuilder("SELECT * FROM ");
      builder.append(TABLE_NAME);
      builder.append(" WHERE USR = '" + cond + "';");
      this.swriter.println(builder.toString());
      this.uwriter.println(builder.toString());
    } else {
      StringBuilder sbuilder = new StringBuilder("SELECT * FROM ");
      sbuilder.append(PUR_VIEW);
      sbuilder.append(" WHERE PUR = '" + cond + "';");
      this.swriter.println(sbuilder.toString());
      StringBuilder ubuilder = new StringBuilder("SELECT * FROM ");
      ubuilder.append(TABLE_NAME);
      ubuilder.append(" WHERE PUR = '" + cond + "';");
      this.uwriter.println(ubuilder.toString());
    }
    return Status.OK;
  }
  
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // Sharded and unshared scan statements are slightly different:
    // one looks up from a table, the other from a view.
    StringBuilder sbuilder = new StringBuilder("SELECT * FROM ");
    sbuilder.append(SCAN_VIEW);
    sbuilder.append(" WHERE ");
    sbuilder.append(PRIMARY_KEY);
    sbuilder.append(" > '");
    sbuilder.append(startkey);
    sbuilder.append("'");
    sbuilder.append(" LIMIT ");
    sbuilder.append(recordcount);
    sbuilder.append(";");
    this.swriter.println(sbuilder.toString());
    StringBuilder ubuilder = new StringBuilder("SELECT * FROM ");
    ubuilder.append(TABLE_NAME);
    ubuilder.append(" WHERE ");
    ubuilder.append(PRIMARY_KEY);
    ubuilder.append(" > '");
    ubuilder.append(startkey);
    ubuilder.append("'");
    ubuilder.append(" ORDER BY ");
    ubuilder.append(PRIMARY_KEY);
    ubuilder.append(" LIMIT ");
    ubuilder.append(recordcount);
    ubuilder.append(";");
    this.uwriter.println(ubuilder.toString());
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(TABLE_NAME);
    builder.append(" VALUES ('");
    builder.append(key);
    builder.append("'");
    for (String col : COLUMNS) {
      builder.append(", '");
      builder.append(values.get(col).toString());
      builder.append("'");
    }
    builder.append(");");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(table);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append(" = '");
    builder.append(key);
    builder.append("';");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status deleteMeta(String table, int fieldnum, String cond, String keymatch) {
    if (!keymatch.equals("key*")) {
      System.out.println("Delete meta error " + keymatch);
      return Status.ERROR;
    }
    
    String columnName = isUsr(fieldnum) ? "USR" : "PUR";
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(columnName);
    builder.append(" = '" + cond + "';");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String value = key;
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());

    StringBuilder builder = new StringBuilder("UPDATE ");
    builder.append(TABLE_NAME);
    builder.append(" SET ");
    builder.append("Data = '" + value + "'");
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append(" = '");
    builder.append(key);
    builder.append("';");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status updateMeta(String table, int fieldnum, String cond, String keymatch,
      String fieldkey, String fieldvalue) {
    if (!keymatch.equals("key*")) {
      System.out.println("Read meta error " + keymatch);
      return Status.ERROR;
    }

    // This is only used in the controller load where everything is uniform.
    // Similarly, we will uniformly select a user here.
    int i = (int) (Math.random() * NUM_USERS);
    String usr = "user" + (i + 1);

    String columnName = isUsr(fieldnum) ? "USR" : "PUR";
    String value = "" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());
    value += "-" + (int) (Integer.MAX_VALUE * Math.random());

    StringBuilder builder = new StringBuilder("UPDATE ");
    builder.append(TABLE_NAME);
    builder.append(" SET ");
    builder.append("Data = '" + value + "'");
    builder.append(" WHERE ");
    builder.append(columnName);
    builder.append(" = '");
    builder.append(cond);
    if (!isUsr(fieldnum)) {
      builder.append("' AND ");
      builder.append("USR = '");
      builder.append(usr);
    }
    builder.append("';");
    this.swriter.println(builder.toString());
    this.uwriter.println(builder.toString());
    return Status.OK;
  }

  @Override
  public Status insertTTL(String table, String key, Map<String, ByteIterator> values, int ttl) {
    return this.insert(table, key, values);
  }
  
  @Override
  public Status verifyTTL(String table, long recordcount) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status readLog(String table, int logcount) {
    return Status.NOT_IMPLEMENTED;
  }
}
