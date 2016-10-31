package benchmark;

import GraphItems.ArtField;
import GraphItems.ArtMovement;
import GraphItems.Location;
import GraphItems.Person;
import com.arangodb.*;
import com.arangodb.entity.*;
import com.arangodb.util.MapBuilder;
import com.github.sommeri.less4j.core.compiler.expressions.strings.StringFormatter;
import sparql.DBPediaSparqlQuerier;
import sparql.dbpediaObjects.DBPediaArtist;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.Normalizer;
import java.util.*;

/**
 * Created by Amit on 20/10/2016.
 */
public class ArtistsTest {
    private static final boolean RESET_MODE = true;
    private static boolean BUILD_MODE = false;

    public static final String dbName = "artDb";
    public static final String graphName = "Art";

    public static final String artistsCollectionName = "Artists";
    public static final String locationsCollectionName = "Location";
    public static final String artMovementCollectionName = "ArtMovements";
    public static final String artFieldCollectionName = "artField";
    public static final String bornInEdgeCollection = "BORN_IN";
    public static final String deathInEdgeCollection = "DIED_IN";
    public static final String memberOfEdgeCollection = "MEMBER_OF";
    public static final String describedByEdgeCollection = "DESCRIBED_BY";


    public static void main(String[] args) throws IOException, ArangoException {
        ArangoDriver arangoDriver = setUpDBPediaArtistsDB(BUILD_MODE, RESET_MODE);

        //Select edges with AQL and short traversing
        try {
            String query = "FOR t IN Artists FILTER t.Description LIKE '%January%' RETURN t";
            Map<String, Object> bindVars = new MapBuilder().get();
            DocumentCursor cursor = arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            Iterator iterator = cursor.entityIterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = (BaseDocument) iterator.next();
                System.out.println("Key: " + aDocument.getDocumentKey() + " | Written By: " + aDocument.getAttribute("Name"));
            }
        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
            System.exit(0);
        }

    }

    public static ArangoDriver setUpDBPediaArtistsDB(boolean isBuildMode, boolean isResetMode) throws ArangoException, IOException {
        int artistsCounter = 0, edgesCounter = 0;
        //Set up adapter
        ArangoDriver arangoDriver = getArangoDriver();
        setUpDB(arangoDriver);

        if(isBuildMode) {
            if (isResetMode)
                arangoDriver.deleteGraph(graphName, true);
            GraphEntity graph = setUpGraph(arangoDriver);
            setUpArtistEdgesAndVertices(arangoDriver, graph);

            Long runStartTime = System.currentTimeMillis();
            List<DBPediaArtist> artists = DBPediaSparqlQuerier.getMostFamousArtists(10000);

            runStartTime = System.currentTimeMillis();

            for (DBPediaArtist artist : artists) {
                artistsCounter++;
                DocumentEntity<DBPediaArtist> artistVertex = getOrCreateVertex(arangoDriver, graphName, artistsCollectionName, artist.getWikiPageID(), artist, DBPediaArtist.class);
                for (String place : artist.getBirthPlace()) {
                    if (place.equals(""))
                        continue;
                    DocumentEntity<Location> birthPlace = getOrCreateVertex(arangoDriver, graphName, locationsCollectionName, place, new Location(place), Location.class);
                    EdgeEntity<String> edgeEntity = arangoDriver.graphCreateEdge(graphName, bornInEdgeCollection, null, artistVertex.getDocumentHandle(), birthPlace.getDocumentHandle());
                    DocumentEntity<BaseDocument> edgeDocument = arangoDriver.getDocument(bornInEdgeCollection, edgeEntity.getDocumentKey(), BaseDocument.class);
                    BaseDocument edge = edgeDocument.getEntity();
                    edge.addAttribute("birth_date", artist.getBirthDate());
                    arangoDriver.updateDocument(edgeDocument.getDocumentHandle(), edge);
                    edgesCounter++;
                }
                for (String place : artist.getDeathPlace()) {
                    if (place.equals(""))
                        continue;
                    DocumentEntity<Location> deathPlace = getOrCreateVertex(arangoDriver, graphName, locationsCollectionName, place, new Location(place), Location.class);
                    EdgeEntity<String> edgeEntity = arangoDriver.graphCreateEdge(graphName, deathInEdgeCollection, null, artistVertex.getDocumentHandle(), deathPlace.getDocumentHandle());
                    DocumentEntity<BaseDocument> edgeDocument = arangoDriver.getDocument(deathInEdgeCollection, edgeEntity.getDocumentKey(), BaseDocument.class);
                    BaseDocument edge = edgeDocument.getEntity();
                    edge.addAttribute("death_date", artist.getDeathDate());
                    arangoDriver.updateDocument(edgeDocument.getDocumentHandle(), edge);
                    edgesCounter++;
                }
                for (String movement : artist.getArtMovements()) {
                    if (movement.equals(""))
                        continue;
                    DocumentEntity<ArtMovement> artMovement = getOrCreateVertex(arangoDriver, graphName, artMovementCollectionName, movement, new ArtMovement(movement), ArtMovement.class);
                    arangoDriver.graphCreateEdge(graphName, memberOfEdgeCollection, null, artistVertex.getDocumentHandle(), artMovement.getDocumentHandle());
                    edgesCounter++;
                }
                for (String field : artist.getArtFields()) {
                    if (field.equals(""))
                        continue;
                    DocumentEntity<ArtField> artField = getOrCreateVertex(arangoDriver, graphName, artFieldCollectionName, field, new ArtField(field), ArtField.class);
                    arangoDriver.graphCreateEdge(graphName, describedByEdgeCollection, null, artistVertex.getDocumentHandle(), artField.getDocumentHandle());
                    edgesCounter++;
                }

                String query = "FOR t IN Artists RETURN t";
                Map<String, Object> bindVars = new MapBuilder().get();
                DocumentCursor cursor = arangoDriver.executeDocumentQuery(query, bindVars, null,
                        BaseDocument.class);
            }
            System.out.format("Added " + artistsCounter + " artists and " +
                    edgesCounter + " edges in %.4f seconds\n", (System.currentTimeMillis() - runStartTime) / 1000.0);
        }

        return arangoDriver;
    }

    private static <T> DocumentEntity<T> getOrCreateVertex(ArangoDriver arangoDriver, String graphName, String collectionName, String key, T verticeObject, Class objectType) throws ArangoException {
        String cleanKey = key.replace("ref%3E ","");
        if(cleanKey.equals("")) return null;
        cleanKey = cleanText(cleanKey);

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

    private static String cleanText(String key) {
        String cleanKey = Normalizer.normalize(key.replace(" ", "_"), Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        return cleanKey;
    }

    private static void setUpArtistEdgesAndVertices(ArangoDriver arangoDriver, GraphEntity graph) throws ArangoException {
        List<String> existingCollections = new LinkedList<String>();
        for(EdgeDefinitionEntity existingEdge : graph.getEdgeDefinitions())
            existingCollections.add(existingEdge.getCollection());

        addCollectionIfNotExists(arangoDriver, existingCollections, bornInEdgeCollection, artistsCollectionName, locationsCollectionName, graphName);
        addCollectionIfNotExists(arangoDriver, existingCollections, deathInEdgeCollection, artistsCollectionName, locationsCollectionName, graphName);
        addCollectionIfNotExists(arangoDriver, existingCollections, memberOfEdgeCollection, artistsCollectionName, artMovementCollectionName, graphName);
        addCollectionIfNotExists(arangoDriver, existingCollections, describedByEdgeCollection, artistsCollectionName, artFieldCollectionName, graphName);
    }

    private static void addCollectionIfNotExists(ArangoDriver arangoDriver, List<String> existingCollections, String bornInEdgeCollection, String artistsCollectionName, String locationsCollectionName, String graphName) throws ArangoException {
        if (!existingCollections.contains(bornInEdgeCollection)) {
            EdgeDefinitionEntity edgeDefinition = new EdgeDefinitionEntity();
            List<String> from = new ArrayList<String>();
            from.add(artistsCollectionName);
            edgeDefinition.setFrom(from);
            List<String> to = new ArrayList<String>();
            to.add(locationsCollectionName);
            edgeDefinition.setTo(to);
            edgeDefinition.setCollection(bornInEdgeCollection);
            arangoDriver.graphCreateEdgeDefinition(graphName, edgeDefinition);
        }
    }

    private static GraphEntity setUpGraph(ArangoDriver arangoDriver) throws IOException {
        GraphEntity graph = null;
        try {
            if(arangoDriver.getGraphList().contains(graphName))
                return arangoDriver.getGraph(graphName);
            graph = arangoDriver.createGraph(graphName,true);
            System.out.println("Graph created successfully");
        } catch (ArangoException e) {
            System.out.println("Failed to delete graph; " + e.getMessage());
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            br.readLine();
        }
        return graph;
    }

    private static void setUpDB(ArangoDriver arangoDriver) {
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

    private static ArangoDriver getArangoDriver() {
        ArangoConfigure configure = new ArangoConfigure();
        configure.init();
        configure.setUser("root");
        configure.setPassword("");
        configure.setArangoHost(new ArangoHost("127.0.0.1",8529));
        configure.init();
        return new ArangoDriver(configure);
    }

}
