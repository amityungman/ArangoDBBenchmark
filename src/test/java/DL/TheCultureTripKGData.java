package DL;

import utils.Delegate;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Handler;

/**
 * Created by Amit on 20/11/2016.
 */
public class TheCultureTripKGData {
    final private static String SPLIT_CHAR = "->";

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        List<Node> nodes = getTCTNodes(NODE_TYPE.STATE, null);
    }

    /***
     * Preforms a certain action on all nodes of certain types
     * @param nodeTypes - The types list
     * @param handler - The handler for the nodes
     */
    public static void doForTCTNodes(List<NODE_TYPE> nodeTypes, Delegate<Node> handler) {
        if (nodeTypes == null)
            return;
        for(NODE_TYPE nodeType : nodeTypes) {
            List<Node> nodes = getTCTNodes(nodeType, null);
            for(Node node : nodes)
                handler.preform(node);
        }
    }

    /***
     * Gets all TCT nodes from the KG_DATA DB from a certain type up to a certain limit
     * @param wantedNodeType - The wanted node type. If null, will bring all types
     * @param limit - The limit. If null, will have no limit
     * @return A list of TCT nodes from the DB, of the wanted type, up to the limit.
     */
    public static List<Node> getTCTNodes(NODE_TYPE wantedNodeType, Integer limit) {
        List<Node> nodes = new LinkedList<Node>();
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(
                    "jdbc:mysql://knowledge-graph.c5caxgbi7m7h.us-east-1.rds.amazonaws.com:3306/kg_data","root","XGKkfdwRFw8irrfKNOTQfTzLEDpk05P5jeMo3OSP");

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select nt.node_id, nt.node_type, nt.creation_time, nt.update_time, \n" +
                    "group_concat(concat(npt.property_key,'/////',npt.property_value,'/////',npt.creation_time,'/////',npt.update_time),'" + SPLIT_CHAR + "') node_properties\n" +
                    "from nodes_tct nt\n" +
                    "join node_properties_tct npt\n" +
                    "on   nt.node_id = npt.node_id\n" +
                    (wantedNodeType == null ? "" : "and  n.node_type = " + wantedNodeType.getValue()) +
                    "group by nt.node_id, nt.node_type, nt.creation_time, nt.update_time\n" +
                    (limit == null ? "" : "limit " + limit));

            while(rs.next()) {
                String nodeID = rs.getString(1);
                NODE_TYPE nodeType = NODE_TYPE.fromInteger(rs.getInt(2));
                String creationDate = rs.getString(3);
                String updateDate = rs.getString(4);

                List<NodeProperty> nodeProperties = new LinkedList<NodeProperty>();
                String propertiesString = rs.getString(5);
                for(String propertyString : propertiesString.split(SPLIT_CHAR)) {
                    String cleanPropertyString = propertyString;
                    if(cleanPropertyString.substring(0,1).equals(","))
                        cleanPropertyString = cleanPropertyString.substring(1);
                    NodeProperty newProperty = new NodeProperty(cleanPropertyString);
                    for(NodeProperty nodeProperty : nodeProperties) {
                        if(!nodeProperty.NodeProperty.equals(newProperty.NodeProperty))
                            continue;
                        if(newProperty.UpdateDate.compareTo(nodeProperty.UpdateDate) > 0) {
                            nodeProperties.remove(nodeProperty);
                        }
                    }
                    nodeProperties.add(newProperty);
                }

                nodes.add(new Node(nodeID, nodeType, nodeProperties, creationDate, updateDate));
            }
        }
        catch(Exception e){ System.out.println(e);}
        finally {
            if(con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return nodes;
    }
}
