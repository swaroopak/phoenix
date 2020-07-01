package org.apache.phoenix.end2end;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.SchemaExtractionTool;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;

import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class SchemaExtractionToolIT extends BaseTest {

    private final String TEST_RESOURCES_DIRECTORY = "src/test/resources/";
    private final String CREATE_INDEX_TREE_FILE = "src/test/resources/CREATE_INDEX_TREE_FILE";
    private final String CREATE_VIEW_INDEX_TREE_FILE = "src/test/resources/CREATE_VIEW_INDEX_TREE_FILE";

    /**
     * Read the contents of the named file into a string
     */
    private String read(String fileName) throws Exception{
        FileInputStream f =  new FileInputStream(fileName);
        BufferedReader r = new BufferedReader (new InputStreamReader(f));
        StringBuilder sb = new StringBuilder();
        String thisLine = null;
        while ((thisLine = r.readLine()) != null) {
            sb.append(thisLine + "\n");
        }
        return sb.toString();
    }

    /**
     * Compare two files.
     */
    private void compareFiles(String actual, String expected) throws Exception {
        Assert.assertEquals("Files did not match; actual result is in " + actual, read(expected), read(actual));
    }

    /**
     * DELETE THE OUTPUT FILE
     */
    private void deleteFile(String testOutputName) throws Exception {
        File f = new File(testOutputName);
        f.delete();
    }

    /**
     * Compare two files.
     */
    public void generateTreeFileAndCompareDiffTest(SchemaExtractionTool set, String tableName, String schemaName, String expectedFile) throws Exception {
        String testOutputName = TEST_RESOURCES_DIRECTORY + "generateTreeFileTest_" + String.valueOf(System.currentTimeMillis());
        try {
            String [] args = {"-tb", tableName, "-s", schemaName, "--tree-file", testOutputName};
            set.run(args);

            compareFiles(testOutputName, expectedFile);
        } finally {
            deleteFile(testOutputName);
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCreateTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " +
                    "TTL=2592000, IMMUTABLE_ROWS=true, DISABLE_MIGRATION=true, DISABLE_SOR=true, DISABLE_WAL=true";
            conn.createStatement().execute(query);
            conn.commit();

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            String [] args = {"-tb", tableName, "-s", schemaName};
            set.run(args);
            String actualProperties = set.output.substring(set.output.lastIndexOf(")")+1).replace(" ","");
            Assert.assertEquals(5, actualProperties.split(",").length);
        }
    }

    @Test
    public void testSaltedTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS=16";
            conn.createStatement().execute(query);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            String actualProperties = set.output.substring(set.output.lastIndexOf(")")+1);
            Assert.assertEquals(true, actualProperties.contains("SALT_BUCKETS=16"));
        }
    }

    @Test
    public void testCreateTableWithPKConstraint() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(15) NOT NULL, " +
                    "c_bigint BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (a_char, b_char, c_bigint)) IMMUTABLE_ROWS=TRUE";
            conn.createStatement().execute(query);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithArrayColumn() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "c_var_array VARCHAR ARRAY, " +
                    "d_char_array CHAR(15) ARRAY[3] CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                    "TTL=2592000, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, DISABLE_TABLE_SOR=true, REPLICATION_SCOPE=1";
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithNonDefaultColumnFamily() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1), " +
                    "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                    "TTL=1209600, IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, DISABLE_TABLE_SOR=true, MULTI_TENANT=true";
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);

            String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC) INCLUDE (v2)";

            String createIndexStatement1 = "CREATE INDEX "+indexName1 + " ON "+pTableFullName+"(v2 DESC) INCLUDE (v1)";

            String createIndexStatement2 = "CREATE INDEX "+indexName2 + " ON "+pTableFullName+"(k)";

            conn.createStatement().execute(createIndexStatement);
            conn.createStatement().execute(createIndexStatement1);
            conn.createStatement().execute(createIndexStatement2);
            conn.commit();
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());

            String [] args = {"-tb", indexName, "-s", schemaName};
            set.run(args);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.output.toUpperCase());

            String [] args1 = {"-tb", indexName1, "-s", schemaName};
            set.run(args1);
            Assert.assertEquals(createIndexStatement1.toUpperCase(), set.output.toUpperCase());

            String [] args2 = {"-tb", indexName2, "-s", schemaName};
            set.run(args2);
            Assert.assertEquals(createIndexStatement2.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateIndexStatementWithFamilyColumn() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, \"av\".\"_\" CHAR(1), v2 VARCHAR)");
            String createIndexStatement = "CREATE INDEX "+ indexName + " ON "+pTableFullName+ "(\"av\".\"_\")";
            conn.createStatement().execute(createIndexStatement);
            conn.commit();

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            String [] args2 = {"-tb", indexName, "-s", schemaName};
            set.run(args2);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateViewStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
            String viewFullName1 = SchemaUtil.getQualifiedTableName(schemaName, viewName+"1");


            String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) AS SELECT * FROM "+pTableFullName;
            String createView1 = "CREATE VIEW "+viewFullName1 + "(id1 BIGINT, id2 BIGINT NOT NULL, id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) AS SELECT * FROM "+pTableFullName;

            conn.createStatement().execute(createView);
            conn.createStatement().execute(createView1);
            conn.commit();
            String [] args = {"-tb", viewName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            Assert.assertEquals(createView.toUpperCase(), set.output.toUpperCase());
        }
    }

    @Test
    public void testCreateViewIndexStatement() throws Exception {
        String tableName = "TEST_TABLE";
        String schemaName = "TEST_SCHEMA";
        String viewName = "TEST_VIEW";
        String childView = "TEST_CHILD_VIEW";
        String indexName = "TEST_INDEX";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_MIGRATION=true,DISABLE_SOR=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
            String childviewName = SchemaUtil.getQualifiedTableName(schemaName, childView);

            String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) AS SELECT * FROM "+pTableFullName;

            String createView1 = "CREATE VIEW "+childviewName + " AS SELECT * FROM "+viewFullName;

            String createIndexStatement = "CREATE INDEX "+indexName + " ON "+childviewName+"(id1) INCLUDE (v1)";

            conn.createStatement().execute(createView);
            conn.createStatement().execute(createView1);
            conn.createStatement().execute(createIndexStatement);
            conn.commit();
            String [] args = {"-tb", indexName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.output.toUpperCase());

            generateTreeFileAndCompareDiffTest(set, tableName, schemaName, CREATE_VIEW_INDEX_TREE_FILE);
        }
    }

    @Test
    public void testCreateIndexTreeFile() throws Exception {
        String tableName = "TEST_TABLE2";
        String schemaName = "TEST_SCHEMA2";
        String indexName = "TEST_INDEX2";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        String createTableStatement = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "c_var_array VARCHAR ARRAY, " +
                "d_char_array CHAR(15) ARRAY[3], " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                "TTL=2592000, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, DISABLE_TABLE_SOR=true, REPLICATION_SCOPE=1";

        String createIndexStatement = "CREATE INDEX "+ indexName + " ON "+pTableFullName+ "(a_char DESC, b_char) INCLUDE (c_var_array, d_char_array)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableStatement);
            conn.createStatement().execute(createIndexStatement);
            conn.commit();

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            generateTreeFileAndCompareDiffTest(set, tableName, schemaName, CREATE_INDEX_TREE_FILE);
        }
    }
}
