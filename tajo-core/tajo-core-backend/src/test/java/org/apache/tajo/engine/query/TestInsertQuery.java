/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestInsertQuery {
  private static TpchTestBase tpch;
  public TestInsertQuery() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @Test
  public final void testInsertOverwrite() throws Exception {
    String tableName ="InsertOverwrite";
    tpch.execute("create table " + tableName +" (col1 int8, col2 int4, col3 float4)");
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    tpch.execute("insert overwrite into " + tableName + " select l_orderkey, l_partkey, l_quantity from lineitem");
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getMeta().getStat().getNumRows().intValue());
  }

  @Test
  public final void testInsertOverwriteSmallerColumns() throws Exception {
    String tableName = "insertoverwritesmallercolumns";
    tpch.execute("create table " + tableName + " (col1 int8, col2 int4, col3 float4)");
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));
    TableDesc originalDesc = catalog.getTableDesc(tableName);

    tpch.execute("insert overwrite into " + tableName + " select l_orderkey from lineitem");
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getMeta().getStat().getNumRows().intValue());
    assertEquals(originalDesc.getMeta().getSchema(), desc.getMeta().getSchema());
  }

  @Test
  public final void testInsertOverwriteWithTargetColumns() throws Exception {
    String tableName = "InsertOverwriteWithTargetColumns";
    tpch.execute("create table " + tableName + " (col1 int8, col2 int4, col3 float4)");
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));
    TableDesc originalDesc = catalog.getTableDesc(tableName);

    tpch.execute("insert overwrite into " + tableName + " (col1, col3) select l_orderkey, l_quantity from lineitem");
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(5, desc.getMeta().getStat().getNumRows().intValue());

    ResultSet res = tpch.execute("select * from " + tableName);
    assertTrue(res.next());
    assertEquals(1, res.getLong(1));
    assertEquals("null", res.getString(2));
    assertTrue(17.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(1, res.getLong(1));
    assertEquals("null", res.getString(2));
    assertTrue(36.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(2, res.getLong(1));
    assertEquals("null", res.getString(2));
    assertTrue(38.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(3, res.getLong(1));
    assertEquals("null", res.getString(2));
    assertTrue(45.0 == res.getFloat(3));

    assertTrue(res.next());
    assertEquals(3, res.getLong(1));
    assertEquals("null", res.getString(2));
    assertTrue(49.0 == res.getFloat(3));

    assertFalse(res.next());
    res.close();

    assertEquals(originalDesc.getMeta().getSchema(), desc.getMeta().getSchema());
  }

  @Test
  public final void testInsertOverwriteWithAsterisk() throws Exception {
    String tableName = "testinsertoverwritewithasterisk";
    tpch.execute("create table " + tableName + " as select * from lineitem");
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    tpch.execute("insert overwrite into " + tableName + " select * from lineitem where l_orderkey = 3");
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(2, desc.getMeta().getStat().getNumRows().intValue());
  }

  @Test
  public final void testInsertOverwriteIntoSelect() throws Exception {
    String tableName = "insertoverwriteintoselect";
    ResultSet res = tpch.execute(
        "create table " + tableName + " as select l_orderkey from lineitem");
    assertFalse(res.next());
    res.close();

    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));
    TableDesc orderKeys = catalog.getTableDesc(tableName);
    assertEquals(5, orderKeys.getMeta().getStat().getNumRows().intValue());

    // this query will result in the two rows.
    res = tpch.execute(
        "insert overwrite into " + tableName + " select l_orderkey from lineitem where l_orderkey = 3");
    assertFalse(res.next());
    res.close();

    assertTrue(catalog.existsTable(tableName));
    orderKeys = catalog.getTableDesc(tableName);
    assertEquals(2, orderKeys.getMeta().getStat().getNumRows().intValue());
  }

  @Test
  public final void testInsertOverwriteCapitalTableName() throws Exception {
    String tableName = "testInsertOverwriteCapitalTableName";
    tpch.execute("create table " + tableName + " as select * from lineitem");
    TajoTestingCluster cluster = tpch.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(tableName));

    tpch.execute("insert overwrite into " + tableName + " select * from lineitem where l_orderkey = 3");
    TableDesc desc = catalog.getTableDesc(tableName);
    assertEquals(2, desc.getMeta().getStat().getNumRows().intValue());
  }

  @Test
  public final void testInsertOverwriteLocation() throws Exception {
    tpch.execute("insert overwrite into location '/tajo-data/testInsertOverwriteCapitalTableName' select * from lineitem where l_orderkey = 3");
    FileSystem fs = FileSystem.get(tpch.getTestingCluster().getConfiguration());
    assertTrue(fs.exists(new Path("/tajo-data/testInsertOverwriteCapitalTableName")));
    assertEquals(1, fs.listStatus(new Path("/tajo-data/testInsertOverwriteCapitalTableName")).length);
  }
}