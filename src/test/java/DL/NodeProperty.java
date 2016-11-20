package DL;

/**
 * Created by Amit on 20/11/2016.
 */
public class NodeProperty {
    NODE_PROPERTY NodeProperty;
    String NodeValue;
    String CreationDate;
    String UpdateDate;

    NodeProperty(NODE_PROPERTY nodeProperty, String nodeValue, String creationDate, String updateDate) {
        NodeProperty = nodeProperty;
        NodeValue = nodeValue;
        CreationDate = creationDate;
        UpdateDate = updateDate;
    }

    public NodeProperty(String propertyString) {
        String[] propertyParts = propertyString.split("/////");

        NodeProperty = NODE_PROPERTY.fromInteger(Integer.parseInt(propertyParts[0]));
        NodeValue = propertyParts[1];
        CreationDate = propertyParts[2];
        UpdateDate = propertyParts[3];
    }
}
