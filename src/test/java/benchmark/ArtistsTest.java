package benchmark;

import GraphItems.ArtField;
import GraphItems.ArtMovement;
import GraphItems.Location;
import GraphItems.Person;
import com.arangodb.*;
import com.arangodb.entity.*;
import com.arangodb.util.MapBuilder;
import com.github.sommeri.less4j.core.compiler.expressions.strings.StringFormatter;
import org.apache.jena.atlas.iterator.Action;
import sparql.DBPediaSparqlQuerier;
import sparql.dbpediaObjects.DBPediaArtist;
import sparql.dbpediaObjects.DBPediaArtwork;

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
    public static final int MAX_ARTISTS_AMOUNT = 1500;
    private static boolean BUILD_MODE = true;

    public static final String dbName = "artDb";
    public static final String graphName = "Art";

    public static final String artistsCollectionName = "Artists";
    public static final String locationsCollectionName = "Locations";
    public static final String artMovementCollectionName = "ArtMovements";
    public static final String artFieldCollectionName = "ArtFields";
    public static final String artworkCollectionName = "Artworks";
    public static final String bornInEdgeCollection = "BORN_IN";
    public static final String deathInEdgeCollection = "DIED_IN";
    public static final String memberOfEdgeCollection = "MEMBER_OF";
    public static final String describedByEdgeCollection = "DESCRIBED_BY";
    public static final String createdByEdgeCollection = "CREATED_BY";


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
                System.out.println("Key: " + aDocument.getDocumentKey() + " | Written By: " + aDocument.getAttribute("name"));
            }
        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
            System.exit(0);
        }

    }

    public static ArangoDriver setUpDBPediaArtistsDB(boolean isBuildMode, boolean isResetMode) throws ArangoException, IOException {
        //Set up adapter
        ArangoDriver arangoDriver = getArangoDriver();
        setUpDB(arangoDriver);

        if(isBuildMode) {
            if (isResetMode)
                arangoDriver.deleteGraph(graphName, true);
            GraphEntity graph = setUpGraph(arangoDriver);
            setUpArtistEdgesAndVertices(arangoDriver, graph);

            arangoDriver.createFulltextIndex(artistsCollectionName, "Description");

            addAllNodes(arangoDriver, new LambdaTest() {
                @Override
                public <T> boolean check(T value) {
                    return true;
                }
            });
        }

        return arangoDriver;
    }

    public static void addAllNodes(ArangoDriver arangoDriver, LambdaTest testAction) throws ArangoException {
        Long runStartTime = System.currentTimeMillis();
        int artistsCounter = 0, edgesCounter = 0;

        System.out.println("Getting artists from DBPedia");
        List<DBPediaArtist> artists = DBPediaSparqlQuerier.getMostFamousArtists(MAX_ARTISTS_AMOUNT);
        System.out.println("Got " + artists.size() + " artists");

        for (DBPediaArtist artist : artists) {
            artistsCounter++;
            if(!testAction.check(artist))
                continue;
            DocumentEntity<DBPediaArtist> artistVertex = getOrCreateVertex(arangoDriver, graphName, artistsCollectionName, artist.getWikiPageID(), artist, DBPediaArtist.class);
            for (String place : artist.getBirthPlace()) {
                if (place.equals(""))
                    continue;
                DocumentEntity<Location> birthPlace = getOrCreateVertex(arangoDriver, graphName, locationsCollectionName, place, new Location(place), Location.class);
                EdgeEntity<String> edgeEntity = arangoDriver.graphCreateEdge(graphName, bornInEdgeCollection, null, artistVertex.getDocumentHandle(), birthPlace.getDocumentHandle());
                DocumentEntity<BaseDocument> edgeDocument = arangoDriver.getDocument(bornInEdgeCollection, edgeEntity.getDocumentKey(), BaseDocument.class);
                BaseDocument edge = edgeDocument.getEntity();
                edge.addAttribute("BirthDate", artist.getBirthDate());
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
                edge.addAttribute("DeathDate", artist.getDeathDate());
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
            try {
                for (DBPediaArtwork artwork : DBPediaSparqlQuerier.getAtristArtwork(artist.getWikiPageID())) {
                    DocumentEntity<DBPediaArtwork> artworkEntity = getOrCreateVertex(arangoDriver, graphName, artworkCollectionName, artwork.getName(), artwork, DBPediaArtwork.class);
                    arangoDriver.graphCreateEdge(graphName, createdByEdgeCollection, null, artworkEntity.getDocumentHandle(), artistVertex.getDocumentHandle());
                    edgesCounter++;
                }
            } catch (Exception e) {
                System.out.println("\u001B[31mCould not get artwork for " + artist.getName() + "\u001B[0m");
            }

            System.out.format("Added " + artistsCounter + "/" + artists.size() + " artists and " +
                    edgesCounter + " edges in %.4f seconds\n", (System.currentTimeMillis() - runStartTime) / 1000.0);
        }
    }

    public static <T> DocumentEntity<T> getOrCreateVertex(ArangoDriver arangoDriver, String graphName, String collectionName, String key, T verticeObject, Class objectType) throws ArangoException {
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
        String cleanKey = key.replace(" ", "_");
        cleanKey = cleanKey.replace("&","and");
        cleanKey = cleanKey.replace("~","_");
        cleanKey = cleanKey.replace("+","_");
        cleanKey = cleanKey.replace("-","_");
        cleanKey = cleanKey.replace("*","_");
        cleanKey = cleanKey.replace("^","_");

        cleanKey = Normalizer.normalize(cleanKey, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "").toUpperCase();
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
        addCollectionIfNotExists(arangoDriver, existingCollections, createdByEdgeCollection, artworkCollectionName, artistsCollectionName, graphName);
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
