package org.apache.phoenix.schema;

import com.google.common.base.Strings;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ViewSchemaTreeNode extends SchemaTreeNode {

    public ViewSchemaTreeNode(PTable view, SchemaExtractionTool tool) {
        super(view, tool);
    }

    @Override
    public void visit() throws Exception {
        this.ddl = tool.extractCreateViewDDL(table);

        // get all indices
        for (PTable index : getIndexes()) {
            children.add(new IndexSchemaTreeNode(index, tool));
        }

        for (PTable view : getViews()) {
            children.add(new ViewSchemaTreeNode(view, tool));
        }

        for (SchemaTreeNode node : children) {
            node.visit();
        }

    }

    private List<PTable> getViews() throws SQLException {
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

    private List<PTable> getIndexes() throws SQLException {
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