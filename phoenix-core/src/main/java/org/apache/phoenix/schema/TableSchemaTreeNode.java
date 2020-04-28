package org.apache.phoenix.schema;

public class TableSchemaTreeNode extends SchemaTreeNode {

    TableSchemaTreeNode(PTable table, SchemaExtractionTool tool) {
        super(table, tool);
    }

    @Override
    void visit() throws Exception {

        this.ddl = tool.extractCreateTableDDL(table);

        for (PTable index : table.getIndexes()) {
            children.add(new IndexSchemaTreeNode(index, tool));
        }

        for (PTable view : table.getViews()) {
            children.add(new ViewSchemaTreeNode(view, tool));
        }

        for (SchemaTreeNode node : children) {
            node.visit();
        }
    }
}
