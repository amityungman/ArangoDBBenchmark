import DL.ArangoDAL;
import DL.NODE_TYPE;
import graphItems.ArtField;
import graphItems.ArtMovement;
import graphItems.Location;
import com.arangodb.*;
import com.arangodb.entity.*;
import com.arangodb.util.MapBuilder;
import sparql.DBPediaSparqlQuerier;
import sparql.dbpediaObjects.DBPediaArtist;
import sparql.dbpediaObjects.DBPediaArtwork;
import utils.LambdaTest;

import java.io.IOException;
import java.util.*;

/**
 * Created by Amit on 20/10/2016.
 */
public class CultureTripTest {
    private static final boolean RESET_MODE = true;
    private static boolean BUILD_MODE = true;

    public static final String dbName = "cultureTripDB";
    public static final String graphName = "CultureTrip";


    public static void main(String[] args) throws IOException, ArangoException {
        ArangoDriver arangoDriver = setUpCultureTripDB(BUILD_MODE, RESET_MODE);

        //Select edges with AQL and short traversing
        try {
            String query = "FOR t IN Artists FILTER t.Description LIKE '%January%' RETURN t";
            Map<String, Object> bindVars = new MapBuilder().get();
            DocumentCursor cursor = arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            Iterator iterator = cursor.entityIterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = (BaseDocument) iterator.next();
                System.out.println("Key: " + aDocument.getDocumentKey() + " | Written By: " + aDocument.getAttribute("name"));
            }
        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
            System.exit(0);
        }
    }

    public static ArangoDriver setUpCultureTripDB(boolean isBuildMode, boolean isResetMode) throws ArangoException, IOException {
        //Set up adapter
        ArangoDriver arangoDriver = ArangoDAL.getArangoDriver();
        ArangoDAL.setUpDB(arangoDriver,dbName);

        if(isBuildMode) {
            if (isResetMode)
                arangoDriver.deleteGraph(graphName, true);
            GraphEntity graph = ArangoDAL.setUpGraph(arangoDriver, graphName);
            for(NODE_TYPE nodeType : NODE_TYPE.values()) {
                arangoDriver.createCollection(nodeType.getName());
            }
            //TODO add relation collections
            addAllNodes(arangoDriver);
            //TODO add all relations
        }

        return arangoDriver;
    }

    public static void addAllNodes(ArangoDriver arangoDriver) throws ArangoException {
        //TODO add all nodes
    }
}
