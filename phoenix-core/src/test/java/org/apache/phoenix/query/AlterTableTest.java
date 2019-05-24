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
package org.apache.phoenix.query;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AlterTableTest extends BaseConnectionlessQueryTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private Connection conn;
    private Properties prop;

    @Before
    public void doMiniSetup() throws SQLException {
        prop = new Properties();
        conn = DriverManager.getConnection(getUrl(), prop);
        conn.createStatement().execute("CREATE TABLE TEST_TABLE(key_column INTEGER NOT NULL, " +
                "value_column BIGINT, CONSTRAINT my_pk PRIMARY KEY (key_column))");
        conn.createStatement().execute("CREATE VIEW TEST_TABLE_GLOBAL_VIEW(name VARCHAR, age BIGINT) AS " +
                "SELECT * FROM TEST_TABLE WHERE key_column = 1");
        conn.createStatement().execute("CREATE INDEX TEST_TABLE_GI ON TEST_TABLE(value_column) include (key_column)");
        conn.createStatement().execute("CREATE LOCAL INDEX TEST_TABLE_LI ON TEST_TABLE(value_column) include (key_column)");
        conn.setAutoCommit(true);
    }

    // Test with ALTER TABLE CASCADE INDEX ALL
    @Test
    public void testAlterTableAddCascadeIndexAll() throws SQLException {
        exception.expect(NotImplementedException.class);
        String query = "ALTER TABLE TEST_TABLE ADD CASCADE INDEX ALL new_column VARCHAR";
        conn.createStatement().execute(query);
    }

    // Test with ALTER VIEW CASCADE INDEX ALL
    @Test
    public void testAlterViewAddCascadeIndexAll() throws SQLException {
        exception.expect(NotImplementedException.class);
        String query = "ALTER VIEW TEST_TABLE_GLOBAL_VIEW ADD CASCADE INDEX ALL new_column VARCHAR";
        conn.createStatement().execute(query);
    }

    // Test without CASCADE INDEX ALL: regular feature
    @Test
    public void testAlterTableAdd() throws SQLException {
        exception.expect(NotImplementedException.class);
        String query = "ALTER TABLE TEST_TABLE ADD new_column VARCHAR";
        conn.createStatement().execute(query);
    }

    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterTableAddCascadeIndex() throws SQLException {
        exception.expect(NotImplementedException.class);
        String query = "ALTER TABLE TEST_TABLE ADD CASCADE INDEX TEST_TABLE_GI new_column VARCHAR";
        conn.createStatement().execute(query);
    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterTableAddCascadeIndexes() throws SQLException {
		exception.expect(NotImplementedException.class);
        String query = "ALTER TABLE TEST_TABLE ADD CASCADE INDEX TEST_TABLE_GI, TEST_TABLE_LI new_column VARCHAR";
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterTableInvalidGrammarI() throws SQLException {
        String query = "ALTER TABLE TEST_TABLE_GLOBAL_VIEW ADD ALL new_column VARCHAR";
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterTableInvalidGrammarII() throws SQLException {
        String query = "ALTER TABLE TEST_TABLE_GLOBAL_VIEW ADD CASCADE TEST_TABLE_GI new_column VARCHAR";
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterTableInvalidGrammarIII() throws SQLException {
        String query = "ALTER TABLE TEST_TABLE_GLOBAL_VIEW ADD INDEX TEST_TABLE_GI new_column VARCHAR";
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }


    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

}
