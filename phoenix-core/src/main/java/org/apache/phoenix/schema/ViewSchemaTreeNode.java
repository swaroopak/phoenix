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
        for (PTable index : this.getIndexes()) {
            children.add(new IndexSchemaTreeNode(index, tool));
        }

        for (PTable view : this.getViews()) {
            children.add(new ViewSchemaTreeNode(view, tool));
        }

        for (SchemaTreeNode node : children) {
            node.visit();
        }
    }
}