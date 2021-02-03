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
import edu.brown.pelton.PeltonJNI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for Pelton.
 */
public class PeltonClient extends DB {
  public void init() throws DBException {
    new PeltonJNI().sayHello();
  }

  public void cleanup() throws DBException {
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  @SuppressWarnings("unused")
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    return Status.ERROR;
  }

  @Override
  public Status readMeta(String table, String cond, String keymatch,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status insertTTL(String table, String key, Map<String, ByteIterator> values, int ttl) {
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.ERROR;
  }

  @Override
  public Status deleteMeta(String table, String condition, String keymatch) {
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status updateMeta(String table, String condition, String keymatch, String fieldname,
      String metadatavalue) {
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  @Override
  public Status verifyTTL(String table, long recordcount) {
    return Status.ERROR;
  }

  @Override
  public Status readLog(String table, int logcount) {
    return Status.ERROR;
  }
}
