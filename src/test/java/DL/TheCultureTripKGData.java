package DL;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Amit on 20/11/2016.
 */
public class TheCultureTripKGData {
    final private static String SPLIT_CHAR = "->";

    public static void main(String args[]) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection(
                "jdbc:mysql://knowledge-graph.c5caxgbi7m7h.us-east-1.rds.amazonaws.com:3306/kg_data","root","XGKkfdwRFw8irrfKNOTQfTzLEDpk05P5jeMo3OSP");
        try {

            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select nt.node_id, nt.node_type, nt.creation_time, nt.update_time, \n" +
                    "group_concat(concat(npt.property_key,'/////',npt.property_value,'/////',npt.creation_time,'/////',npt.update_time),'" + SPLIT_CHAR + "') node_properties\n" +
                    "from nodes_tct nt\n" +
                    "join node_properties_tct npt\n" +
                    "on   nt.node_id = npt.node_id\n" +
                    "group by nt.node_id, nt.node_type, nt.creation_time, nt.update_time\n" +
                    "limit 10");

            List<Node> nodes = new LinkedList<Node>();
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
                    nodeProperties.add(new NodeProperty(cleanPropertyString));
                }

                nodes.add(new Node(nodeID, nodeType, nodeProperties, creationDate, updateDate));
            }
        }
        catch(Exception e){ System.out.println(e);}
        finally {
            con.close();
        }
    }
}
