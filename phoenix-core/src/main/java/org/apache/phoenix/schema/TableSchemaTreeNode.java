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
}
