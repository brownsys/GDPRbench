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
import com.yahoo.ycsb.StringByteIterator;

// JDBC
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for SQLITE.
 */
public class SQLiteClient extends DB {
  /** Name of database (and DB file). */
  public static final String DB_NAME_PROPERTY = "sqlite.dbname";
  /** Table schema configuration. */
  public static final String TABLE_NAME = "usertable";
  public static final String PRIMARY_KEY = "YCSB_KEY";
  public static final String METADATA_COLUMN = "PUR";
  public static final int FIELD_COUNT = 10;
  public static final String COLUMN_PREFIX = "FIELD";
  public static final String NULL_VALUE = "NULL";

  /** SQL for table creation. */
  public static final String DROP_INDEX_SQL = "DROP INDEX IF EXISTS pur_index;";
  public static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS usertable;";
  public static final String CREATE_TABLE_SQL =
      "CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY,\n"
      + "  DEC VARCHAR, USR VARCHAR,\n" + "  SRC VARCHAR, OBJ VARCHAR,\n"
      + "  CAT VARCHAR, ACL VARCHAR,\n" + "  Data VARCHAR, PUR VARCHAR,\n"
      + "  SHR VARCHAR, TTL VARCHAR);";
  public static final String CREATE_INDEX_SQL =
      "CREATE INDEX pur_index ON usertable(PUR);";
  public static final List<String> COLUMNS = Arrays.asList(
      new String[] {"DEC", "USR", "SRC", "OBJ", "CAT", "ACL", "Data", "PUR", "SHR", "TTL"});

  private Connection connection;

  public void init() throws DBException {
    Properties props = getProperties();

    try {
      Class.forName("org.sqlite.JDBC");
      // Create connection.
      this.connection = DriverManager
          .getConnection("jdbc:sqlite:" + props.getProperty(DB_NAME_PROPERTY));
      // Drop then create table.
      Statement stmt = this.connection.createStatement();
      stmt.executeUpdate(DROP_INDEX_SQL);
      stmt.executeUpdate(DROP_TABLE_SQL);
      stmt.executeUpdate(CREATE_TABLE_SQL);
      stmt.executeUpdate(CREATE_INDEX_SQL);
      System.out.println("table " + TABLE_NAME + " created!");
    } catch (ClassNotFoundException e) {
      throw new DBException("Class not found", e);
    } catch (SQLException e) {
      throw new DBException("SQLException", e);
    }
  }

  public void cleanup() throws DBException {
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (SQLException e) {
        throw new DBException("SQLException During Close", e);
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("=\"");
    builder.append(key);
    builder.append("\";");
    try {
      Statement stmt = this.connection.createStatement();
      ResultSet output = stmt.executeQuery(builder.toString());
      if (result != null && fields != null) {
        while (output.next()) {
          for (String field : fields) {
            String value = output.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      output.close();
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    if (result.size() == 0) {
      return Status.NOT_FOUND;
    }
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
    builder.append("=\"");
    builder.append(cond);
    builder.append("\";");

    try {
      Statement stmt = this.connection.createStatement();
      ResultSet output = stmt.executeQuery(builder.toString());
      while (output.next()) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for (String col : COLUMNS) {
          values.put(col,  new StringByteIterator(output.getString(col)));
        }
        result.add(values);
      }
      output.close();
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    if (result.size() == 0) {
      return Status.NOT_FOUND;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(table);
    builder.append(" VALUES(\"");
    builder.append(key);
    builder.append("\"");
    for (String col : COLUMNS) {
      builder.append(",\"");
      builder.append(values.get(col).toString());
      builder.append("\"");
    }
    builder.append(");");
    
    try {
      Statement stmt = this.connection.createStatement();
      if (stmt.executeUpdate(builder.toString()) == 1) {
        return Status.OK;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.UNEXPECTED_STATE;
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
    builder.append("=\"");
    builder.append(key);
    builder.append("\";");

    try {
      Statement stmt = this.connection.createStatement();
      int result = stmt.executeUpdate(builder.toString());
      if (result == 1) {
        return Status.OK;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.UNEXPECTED_STATE;
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
    builder.append("=\"");
    builder.append(condition);
    builder.append("\";");

    try {
      Statement stmt = this.connection.createStatement();
      int result = stmt.executeUpdate(builder.toString());
      return Status.OK;
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    StringBuilder builder = new StringBuilder("UPDAET ");
    builder.append(table);
    builder.append(" SET ");
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
      builder.append(e.getKey());
      builder.append("=\"");
      builder.append(e.getValue());
      builder.append("\",");
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("=\"");
    builder.append(key);
    builder.append("\";");

    try {
      Statement stmt = this.connection.createStatement();
      if (stmt.executeUpdate(builder.toString()) == 1) {
        return Status.OK;
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.UNEXPECTED_STATE;
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
    builder.append("=\"");
    builder.append(metadatavalue);
    builder.append("\" WHERE ");
    builder.append(METADATA_COLUMN);
    builder.append("=\"");
    builder.append(condition);
    builder.append("\";");

    try {
      Statement stmt = this.connection.createStatement();
      stmt.executeUpdate(builder.toString());
      return Status.OK;
    } catch (SQLException e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
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
