package DL;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.ArangoHost;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.entity.EdgeDefinitionEntity;
import com.arangodb.entity.GraphEntity;
import utils.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Amit on 28/11/2016.
 */
public class ArangoDAL {

    public static GraphEntity setUpGraph(ArangoDriver arangoDriver, String graphName) throws IOException {
        GraphEntity graph = null;
        try {
            if(arangoDriver.getGraphList().contains(graphName))
                return arangoDriver.getGraph(graphName);
            graph = arangoDriver.createGraph(graphName,true);
        } catch (ArangoException e) {
            System.out.println("Failed to delete graph; " + e.getMessage());
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        return graph;
    }

    public static void setUpDB(ArangoDriver arangoDriver, String dbName) {
        try {
            if(!arangoDriver.getDatabases().getResult().contains(dbName)) {
                arangoDriver.createDatabase(dbName);
                System.out.println("Database created: " + dbName);
            }
        } catch (Exception e) {
            System.out.println("Failed to create database " + dbName + "; " + e.getMessage());
        }
        arangoDriver.setDefaultDatabase(dbName);
    }

    public static ArangoDriver getArangoDriver() {
        ArangoConfigure configure = new ArangoConfigure();
        configure.init();
        configure.setUser("root");
        configure.setPassword("");
        configure.setArangoHost(new ArangoHost("127.0.0.1",8529));
        configure.init();
        return new ArangoDriver(configure);
    }

    public static <T> DocumentEntity<T> getOrCreateVertex(ArangoDriver arangoDriver, String graphName, String collectionName, String key, T verticeObject, Class objectType) throws ArangoException {
        String cleanKey = key.replace("ref%3E ","");
        if(cleanKey.equals("")) return null;
        cleanKey = StringUtils.cleanText(cleanKey);

        DocumentEntity<T> tVertexEntity = null;
        try {
            if (arangoDriver.exists(collectionName, cleanKey))
                tVertexEntity = arangoDriver.getDocument(collectionName, cleanKey, objectType);
            else
                tVertexEntity = arangoDriver.graphCreateVertex(graphName, collectionName, cleanKey, verticeObject, true);
        } catch(ArangoException e) {
            System.out.println("Collection: " + collectionName + " Key: " + key + " | CleanKey: " + cleanKey);
            throw e;
        }
        return tVertexEntity;
    }

    public static void addEdgeCollectionIfNotExists(ArangoDriver arangoDriver, List<String> existingCollections, String edgeCollection, String collectionName, String bCollectionName, String graphName) throws ArangoException {
        if (!existingCollections.contains(edgeCollection)) {
            EdgeDefinitionEntity edgeDefinition = new EdgeDefinitionEntity();
            List<String> from = new ArrayList<String>();
            from.add(collectionName);
            edgeDefinition.setFrom(from);
            List<String> to = new ArrayList<String>();
            to.add(bCollectionName);
            edgeDefinition.setTo(to);
            edgeDefinition.setCollection(edgeCollection);
            arangoDriver.graphCreateEdgeDefinition(graphName, edgeDefinition);
        }
    }
}
