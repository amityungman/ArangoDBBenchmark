package DL;

import java.util.List;

/**
 * Created by Amit on 20/11/2016.
 */
public class Node {
    String NodeID;
    NODE_TYPE NodeType;
    List<NodeProperty> NodeProperties;
    String CreationDate;
    String UpdateDate;

    public Node(String nodeID, NODE_TYPE nodeType, List<NodeProperty> nodeProperties, String creationDate, String updateDate) {
        NodeID = nodeID;
        NodeType = nodeType;
        NodeProperties = nodeProperties;
        CreationDate = creationDate;
        UpdateDate = updateDate;
    }
}
