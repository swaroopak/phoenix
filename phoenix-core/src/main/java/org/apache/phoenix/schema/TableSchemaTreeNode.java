package org.apache.phoenix.schema;

import org.apache.tephra.TxConstants;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableSchemaTreeNode extends SchemaTreeNode {

    TableSchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        super(table, tool);
    }

    @Override
    void visit() throws Exception {

        this.ddl = tool.extractCreateTableDDL(table);

        // get all indices
        for (PTable index : table.getIndexes()) {
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
}
