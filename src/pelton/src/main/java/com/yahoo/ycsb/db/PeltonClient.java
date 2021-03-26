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

import edu.brown.pelton.PeltonJNI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for Pelton.
 */
public class PeltonClient extends DB {
  /** Name of database (and DB file). */
  public static final String DB_DIR_PROPERTY = "pelton.dbdir";
  /** Table schema configuration. */
  public static final String TABLE_NAME = "usertable";
  public static final String PRIMARY_KEY = "YCSB_KEY";
  public static final String METADATA_COLUMN = "PUR";
  public static final String SCAN_VIEW = "scan_view";

  /** SQL for table creation. */
  public static final String[] CREATE_TABLE_SQL = new String[] {
      "CREATE TABLE main(ID VARCHAR PRIMARY KEY, PII_attr VARCHAR);",
      "CREATE TABLE usertable(YCSB_KEY VARCHAR PRIMARY KEY,"
        + " DEC VARCHAR, USR VARCHAR, SRC VARCHAR, OBJ VARCHAR,"
        + " CAT VARCHAR, ACL VARCHAR, Data VARCHAR, PUR VARCHAR,"
        + " SHR VARCHAR, TTL VARCHAR, FOREIGN KEY(PUR) REFERENCES main(ID));",
      "INSERT INTO main VALUES ('PUR=ads++++++++++++++++++++++++++++++++++++++++++++"
        + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'ads');",
      "INSERT INTO main VALUES ('PUR=2fa++++++++++++++++++++++++++++++++++++++++++++"
        + "+++++++++++++++++++++++++++++++++++++++++++++++++', '2fa');",
      "INSERT INTO main VALUES ('PUR=msg++++++++++++++++++++++++++++++++++++++++++++"
        + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'msg');",
      "INSERT INTO main VALUES ('PUR=backup+++++++++++++++++++++++++++++++++++++++++"
        + "+++++++++++++++++++++++++++++++++++++++++++++++++', 'backup');",
      "CREATE VIEW scan_view AS '\"SELECT * FROM usertable ORDER BY YCSB_KEY LIMIT ?\"';"};
  public static final List<String> COLUMNS = Arrays.asList(
      new String[] {"DEC", "USR", "SRC", "OBJ", "CAT", "ACL", "Data", "PUR", "SHR", "TTL"});

  private PeltonJNI pelton;

  public void init() throws DBException {
    Properties props = getProperties();
    this.pelton = new PeltonJNI(props.getProperty(DB_DIR_PROPERTY));

    if (this.pelton.ExecuteDDL(CREATE_TABLE_SQL[0])) {
      for (int i = 1; i < CREATE_TABLE_SQL.length; i++) {
        if (!this.pelton.ExecuteDDL(CREATE_TABLE_SQL[i])) {
          throw new DBException("Statement failed:\n" + CREATE_TABLE_SQL[i]);
        }
      }
    }

    System.out.println("Pelton Initialized...");
  }

  public void cleanup() throws DBException {
    this.pelton.Close();
    System.out.println("Pelton Closed...");
  }
  
  private HashMap<String, Integer> mapColumns(String[] order) {
    HashMap<String, Integer> result = new HashMap<String, Integer>();
    for (int i = 0; i < order.length; i++) {
      result.put(order[i], i);
    }
    return result;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    // System.out.println("read");
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(TABLE_NAME);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("='");
    builder.append(key);
    builder.append("';");

    ArrayList<String[]> output = this.pelton.ExecuteQuery(builder.toString());
    if (output == null) {
      return Status.ERROR;
    }
    if (output.size() < 2) {
      return Status.NOT_FOUND;
    }
    if (output.size() > 2) {
      return Status.UNEXPECTED_STATE;
    }
    
    HashMap<String, Integer> indices = mapColumns(output.get(0));
    Collection<String> cols = fields != null ? fields : COLUMNS;
    for (String col : cols) {
      String val = output.get(1)[indices.get(col)];
      result.put(col, new StringByteIterator(val));
    }
    return Status.OK;
  }

  @Override
  public Status readMeta(String table, String cond, String keymatch,
      Vector<HashMap<String, ByteIterator>> result) {
    // System.out.println("readMeta");
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
    
    ArrayList<String[]> output = this.pelton.ExecuteQuery(builder.toString());
    if (output == null) {
      return Status.ERROR;
    }
    if (output.size() < 2) {
      return Status.NOT_FOUND;
    }

    HashMap<String, Integer> indices = mapColumns(output.get(0));
    for (int i = 1; i < output.size(); i++) {
      HashMap<String, ByteIterator> oneResult = new HashMap<String, ByteIterator>();
      for (String col : COLUMNS) {
        String val = output.get(i)[indices.get(col)];
        oneResult.put(col, new StringByteIterator(val));
      }
      result.add(oneResult);
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // System.out.println("insert");
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
    
    if (this.pelton.ExecuteUpdate(builder.toString()) == 1) {
      return Status.OK;
    }
    return Status.UNEXPECTED_STATE;
  }

  @Override
  public Status insertTTL(String table, String key, Map<String, ByteIterator> values, int ttl) {
    return this.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    // System.out.println("delete");
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(table);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append("='");
    builder.append(key);
    builder.append("';");

    int result = this.pelton.ExecuteUpdate(builder.toString());
    if (result == 1) {
      return Status.OK;
    } else if (result == 0) {
      return Status.NOT_FOUND;
    } else {
      return Status.UNEXPECTED_STATE;        
    }
  }

  @Override
  public Status deleteMeta(String table, String condition, String keymatch) {
    // System.out.println("deleteMeta");
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

    if (this.pelton.ExecuteUpdate(builder.toString()) > 0) {
      return Status.OK;
    }
    return Status.NOT_FOUND;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // System.out.println("update");
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

    int result = this.pelton.ExecuteUpdate(builder.toString());
    if (result == 1) {
      return Status.OK;
    } else if (result == 0) {
      return Status.NOT_FOUND;
    } else {
      return Status.UNEXPECTED_STATE;        
    }
  }

  @Override
  public Status updateMeta(String table, String condition, String keymatch, String fieldname,
      String metadatavalue) {
    // System.out.println("updateMeta");
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

    if (this.pelton.ExecuteUpdate(builder.toString()) > 0) {
      return Status.OK;
    }
    return Status.NOT_FOUND;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    // System.out.println("scan");
    StringBuilder builder = new StringBuilder("SELECT * FROM ");
    builder.append(SCAN_VIEW);
    builder.append(" WHERE ");
    builder.append(PRIMARY_KEY);
    builder.append(">'");
    builder.append(startkey);
    builder.append(" LIMIT ");
    builder.append(recordcount);
    builder.append(";");

    ArrayList<String[]> output = this.pelton.ExecuteQuery(builder.toString());
    if (output == null) {
      return Status.ERROR;
    }
    if (output.size() < 2) {
      return Status.NOT_FOUND;
    }

    Collection<String> cols = fields != null ? fields : COLUMNS;
    HashMap<String, Integer> indices = mapColumns(output.get(0));
    for (int i = 1; i < output.size(); i++) {
      HashMap<String, ByteIterator> oneResult = new HashMap<String, ByteIterator>();
      for (String col : cols) {
        String val = output.get(i)[indices.get(col)];
        oneResult.put(col, new StringByteIterator(val));
      }
      result.add(oneResult);
    }
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
