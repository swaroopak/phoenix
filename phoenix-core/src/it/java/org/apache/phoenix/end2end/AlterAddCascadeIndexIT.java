/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.end2end;

import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.apache.phoenix.schema.MetaDataClient.INCORRECT_INDEX_NAME_S;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class AlterAddCascadeIndexIT extends ParallelStatsDisabledIT {

    public static final String SYNTAX_ERROR = "Syntax error";
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private static Connection conn;
    private Properties prop;
    private boolean isViewIndex;
    private String phoenixObjectName;
    private String indexesName;
    private final String tableDDLOptions;


    public AlterAddCascadeIndexIT(boolean isViewIndex, boolean mutable) {
        this.isViewIndex = isViewIndex;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameters(name="AlterAddCascadeIndexIT_isViewIndex={0},mutable={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { true, true},
            { true, false},
            { false, true},
            { false, false},
        });
    }

    @Before
    public void setup() throws SQLException {
        prop = new Properties();
        conn = DriverManager.getConnection(getUrl(), prop);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS test.us_population (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city)) " + tableDDLOptions);

        if(isViewIndex) {
            conn.createStatement().execute("CREATE VIEW IF NOT EXISTS test.us_population_gv" +
                    "(city_area INTEGER, avg_fam_size INTEGER) AS " +
                    "SELECT * FROM test.us_population WHERE state = 'CA'");

            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gv_gi ON " +
                    "test.us_population_gv (city_area) INCLUDE (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gv_gi_2 ON " +
                    "test.us_population_gv (avg_fam_size) INCLUDE (population)");
            phoenixObjectName = "test.us_population_gv";
            indexesName = "us_population_gv_gi, us_population_gv_gi_2";
        } else {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gi ON " +
                    "test.us_population (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gi_2 ON " +
                    "test.us_population (state, population)");
            phoenixObjectName = "test.us_population";
            indexesName = "us_population_gi, us_population_gi_2";
        }
    }

    // Test with ALTER TABLE CASCADE INDEX ALL
    @Test
    public void testAlterDBOAddCascadeIndexAll() throws Exception {
        String query = "ALTER " +(isViewIndex? "VIEW " : "TABLE ") + phoenixObjectName +" ADD new_column VARCHAR(32) CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column", PVarchar.INSTANCE.getSqlType(), 32)};
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 5, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 4, columnArray);
        }
    }

    // Test with ALTER VIEW CASCADE INDEX ALL
    @Test
    public void testAlterDBOAddCascadeIndexAllUpsert() throws Exception {
        String query = "ALTER " +(isViewIndex? "VIEW " : "TABLE ") + phoenixObjectName +" ADD new_column_3 VARCHAR(64) CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        PreparedStatement ps;
        if(isViewIndex) {
            ps = conn.prepareStatement("UPSERT INTO test.us_population_gv(state,city,population,city_area,avg_fam_size,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,1300,4,'test_column')");
        } else {
            ps = conn.prepareStatement("UPSERT INTO test.us_population(state,city,population,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,'test_column')");
        }
        ps.executeUpdate();
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_3", PVarchar.INSTANCE.getSqlType(), 64)};
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 5, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 4, columnArray);
        }

    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeIndex() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewIndex? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT CASCADE INDEX " + indexesName.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 4, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 3, columnArray);
        }

    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexTwoCols() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("new_column_2", PDouble.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexesName.split(",")[0];
        conn.createStatement().execute(query);
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 7, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 4, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 3, columnArray);
        }

    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexes() throws Exception {
        ColumnInfo [] columnArray = {new ColumnInfo("new_column_1", PDouble.INSTANCE.getSqlType())};
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX " + indexesName;
        conn.createStatement().execute(query);
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 5, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 4, columnArray);
        }
    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexesTwoColumns() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType()),
                new ColumnInfo("new_column_2", PDouble.INSTANCE.getSqlType())};

        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 FLOAT, new_column_2 DOUBLE CASCADE INDEX " + indexesName;
        conn.createStatement().execute(query);
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 7, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 6, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 5, columnArray);
        }

    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexesWithCF() throws Exception {
        ColumnInfo [] columnArray =  {new ColumnInfo("cf.new_column_2", PChar.INSTANCE.getSqlType(), 3)};
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD cf.new_column_2 CHAR(3) CASCADE INDEX " + indexesName;
        conn.createStatement().execute(query);
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 6, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 5, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 4, columnArray);
        }

    }
    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarI() throws SQLException {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName + " ADD new_column VARCHAR ALL";
        exception.expectMessage(SYNTAX_ERROR);
        conn.createStatement().execute(query);

    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarII() throws SQLException {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR CASCADE " + indexesName.split(",")[0];
        exception.expectMessage(SYNTAX_ERROR);
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarIII() throws SQLException {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ") + phoenixObjectName
                + " ADD new_column VARCHAR INDEX " + indexesName.split(",")[0];
        exception.expectMessage(SYNTAX_ERROR);
        conn.createStatement().execute(query);
    }

    // Exception for incorrect index name
    @Test
    public void testAlterDBOIncorrectIndexName() throws SQLException {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME";
        exception.expectMessage(INCORRECT_INDEX_NAME_S);
        conn.createStatement().execute(query);
    }

    // Exception for incorrect index name
    @Test
    public void testAlterDBOIncorrectIndexNameCombination() throws Exception {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX INCORRECT_NAME, "+indexesName;
        exception.expectMessage(INCORRECT_INDEX_NAME_S);
        conn.createStatement().execute(query);
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        //confirm that column counts didn't change
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 4, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 3, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 3, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 3, columnArray);
        }
    }

    // Exception for incorrect index name
    @Test
    public void testAlterDBOFailureScenario() throws Exception {
        String query = "ALTER " + (isViewIndex ? "VIEW " : "TABLE ")
                + phoenixObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX "+indexesName;
        getUtility().getConfiguration().setBoolean("phoenix.client.failure.flag", true);
    //    exception.expect(RuntimeException.class);
        conn.createStatement().execute(query);
        ColumnInfo [] columnArray =  {new ColumnInfo("new_column_1", PFloat.INSTANCE.getSqlType())};
        //confirm that column counts didn't change
        if(isViewIndex) {
            assertDBODefinition(conn, phoenixObjectName, PTableType.VIEW, 5, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi", PTableType.INDEX, 4, columnArray);
            assertDBODefinition(conn, "test.us_population_gv_gi_2", PTableType.INDEX, 4, columnArray);
        } else {
            assertDBODefinition(conn, phoenixObjectName, PTableType.TABLE, 3, columnArray);
            assertDBODefinition(conn, "test.us_population_gi", PTableType.INDEX, 3, columnArray);
            assertDBODefinition(conn, "test.us_population_gi_2", PTableType.INDEX, 3, columnArray);
        }

    }

    @After
    public void teardown() throws SQLException {
        if (isViewIndex) {
            conn.createStatement().execute("DROP INDEX IF EXISTS us_population_gv_gi ON test.us_population_gv");
            conn.createStatement().execute("DROP INDEX IF EXISTS us_population_gv_gi_2 ON test.us_population_gv");
        } else {
            conn.createStatement().execute("DROP INDEX IF EXISTS us_population_gi ON test.us_population");
            conn.createStatement().execute("DROP INDEX IF EXISTS us_population_gi_2 ON test.us_population");
        }
        conn.createStatement().execute("DROP VIEW IF EXISTS test.us_population_gv");
        conn.createStatement().execute("DROP TABLE IF EXISTS test.us_population");
    }


    private void assertDBODefinition(Connection conn, String phoenixObjectName, PTableType pTableType, int baseColumnCount,  ColumnInfo [] columnInfo)
            throws Exception {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(phoenixObjectName);
        String tableName = SchemaUtil.getTableNameFromFullName(phoenixObjectName);
        PreparedStatement p = conn.prepareStatement("SELECT * FROM \"SYSTEM\".\"CATALOG\" WHERE TABLE_SCHEM=? AND TABLE_NAME=? AND TABLE_TYPE=?");
        p.setString(1, schemaName.toUpperCase());
        p.setString(2, tableName.toUpperCase());
        p.setString(3, pTableType.getSerializedValue());
        String output = AlterTableWithViewsIT.getSystemCatalogEntriesForTable(conn, phoenixObjectName, "Mismatch in ColumnCount");
        ResultSet rs = p.executeQuery();
        assertTrue(rs.next());
        assertEquals(output, baseColumnCount, rs.getInt("COLUMN_COUNT"));
        for(ColumnInfo column : columnInfo) {
            assertEquals(output, baseColumnCount, rs.getInt("COLUMN_COUNT"));
        }
        rs.close();
    }

}
