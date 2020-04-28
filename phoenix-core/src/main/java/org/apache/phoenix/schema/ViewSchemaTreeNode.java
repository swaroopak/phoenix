package org.apache.phoenix.schema;

public class ViewSchemaTreeNode extends TableSchemaTreeNode {

    public ViewSchemaTreeNode(PTable view, SchemaExtractionTool tool) {
        super(view, tool);
    }
}