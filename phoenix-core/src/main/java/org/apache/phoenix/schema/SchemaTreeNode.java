package org.apache.phoenix.schema;

import com.google.common.base.Strings;
import org.apache.phoenix.util.SchemaUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

abstract class SchemaTreeNode {
    protected SchemaExtractionTool tool;
    protected PTable table;
    protected String ddl;
    protected ArrayList<SchemaTreeNode> children;

    private static final String NAME = "name";
    private static final String TYPE = "type";
    private static final String DDL = "ddl";
    private static final String CHILDREN = "children";

    SchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        this.tool = tool;
        this.table = table;
        this.children = new ArrayList<>();
    }

    abstract void visit() throws Exception;

    JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(NAME, table.getName().getString());
        json.put(TYPE, table.getType().toString());
        json.put(DDL, ddl);
        if (children.size() > 0) {
            JSONArray jsonChildren = new JSONArray();
            for (SchemaTreeNode node : children) {
                jsonChildren.put(node.toJSON());
            }
            json.put(CHILDREN, jsonChildren);
        }
        return json;
    }

    protected List<PTable> getViews() throws SQLException {
        List<PTable> pTables = new ArrayList<>();
        try (Connection conn = tool.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT COLUMN_FAMILY FROM SYSTEM.CHILD_LINK "
                    + "WHERE TABLE_SCHEM=\'"+table.getSchemaName().getString()+"\'"
                    +" AND TABLE_NAME=\'"+table.getTableName().getString()+"\'");
            while(rs.next()) {
                pTables.add(tool.getPTable(rs.getString("COLUMN_FAMILY")));
            }
        }
        return pTables;
    }

    protected List<PTable> getIndexes() throws SQLException {
        String tableName = table.getTableName().getString();
        String schemaName = table.getSchemaName().getString();
        List<PTable> pTables = new ArrayList<>();
        try (Connection conn = tool.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT DISTINCT COLUMN_FAMILY FROM "
                    + "SYSTEM.CATALOG "
                    + "WHERE TABLE_NAME = \'" + tableName + "\'"
                    + (!Strings.isNullOrEmpty(schemaName) ? " AND TABLE_SCHEM = \'"
                    + schemaName + "\'" : "")
                    + " AND LINK_TYPE = " + PTable.LinkType.INDEX_TABLE.getSerializedValue());
            while(rs.next()) {
                pTables.add(tool.getPTable(SchemaUtil.getQualifiedTableName(schemaName, rs.getString("COLUMN_FAMILY"))));
            }
        }
        return pTables;
    }
}
