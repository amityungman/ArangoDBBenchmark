package benchmark;

import DL.ArangoDAL;
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
public class ArtistsTest {
    private static final boolean RESET_MODE = true;
    public static final int MAX_ARTISTS_AMOUNT = 5000;
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
        ArangoDriver arangoDriver = ArangoDAL.getArangoDriver();
        ArangoDAL.setUpDB(arangoDriver, dbName);

        if(isBuildMode) {
            if (isResetMode)
                arangoDriver.deleteGraph(graphName, true);
            GraphEntity graph = ArangoDAL.setUpGraph(arangoDriver, graphName);
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
        Long DBPediaTime = 0l, DBPediaTempTime = 0l;
        Long runStartTime = System.currentTimeMillis();
        int artistsCounter = 0, edgesCounter = 0;

        System.out.println("Getting artists from DBPedia");
        DBPediaTime = System.currentTimeMillis();
        List<DBPediaArtist> artists = DBPediaSparqlQuerier.getMostFamousArtists(MAX_ARTISTS_AMOUNT);
        DBPediaTime = System.currentTimeMillis() - DBPediaTime;
        System.out.println("Got " + artists.size() + " artists");

        for (DBPediaArtist artist : artists) {
            artistsCounter++;
            if(!testAction.check(artist))
                continue;
            DocumentEntity<DBPediaArtist> artistVertex = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, artistsCollectionName, artist.getWikiPageID(), artist, DBPediaArtist.class);
            for (String place : artist.getBirthPlace()) {
                if (place.equals(""))
                    continue;
                DocumentEntity<Location> birthPlace = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, locationsCollectionName, place, new Location(place), Location.class);
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
                DocumentEntity<Location> deathPlace = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, locationsCollectionName, place, new Location(place), Location.class);
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
                DocumentEntity<ArtMovement> artMovement = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, artMovementCollectionName, movement, new ArtMovement(movement), ArtMovement.class);
                arangoDriver.graphCreateEdge(graphName, memberOfEdgeCollection, null, artistVertex.getDocumentHandle(), artMovement.getDocumentHandle());
                edgesCounter++;
            }
            for (String field : artist.getArtFields()) {
                if (field.equals(""))
                    continue;
                DocumentEntity<ArtField> artField = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, artFieldCollectionName, field, new ArtField(field), ArtField.class);
                arangoDriver.graphCreateEdge(graphName, describedByEdgeCollection, null, artistVertex.getDocumentHandle(), artField.getDocumentHandle());
                edgesCounter++;
            }
            try {
                DBPediaTempTime = System.currentTimeMillis();
                List<DBPediaArtwork> artworks = DBPediaSparqlQuerier.getAtristArtwork(artist.getWikiPageID());
                DBPediaTime += System.currentTimeMillis() - DBPediaTempTime;
                for (DBPediaArtwork artwork : artworks) {
                    DocumentEntity<DBPediaArtwork> artworkEntity = ArangoDAL.getOrCreateVertex(arangoDriver, graphName, artworkCollectionName, artwork.getName(), artwork, DBPediaArtwork.class);
                    arangoDriver.graphCreateEdge(graphName, createdByEdgeCollection, null, artworkEntity.getDocumentHandle(), artistVertex.getDocumentHandle());
                    edgesCounter++;
                }
            } catch (Exception e) {
                System.out.println("\u001B[31mCould not get artwork for " + artist.getName() + "\u001B[0m");
            }

            System.out.format("Added " + artistsCounter + "/" + artists.size() + " artists and " +
                    edgesCounter + " edges in %.4f seconds\n", (System.currentTimeMillis() - runStartTime) / 1000.0);
        }

        System.out.format("Total DBPedia time was \u001B[32m[%.4f seconds]\u001B[0m\n", DBPediaTime/ 1000.0);
    }

    private static void setUpArtistEdgesAndVertices(ArangoDriver arangoDriver, GraphEntity graph) throws ArangoException {
        List<String> existingCollections = new LinkedList<String>();
        for(EdgeDefinitionEntity existingEdge : graph.getEdgeDefinitions())
            existingCollections.add(existingEdge.getCollection());

        ArangoDAL.addEdgeCollectionIfNotExists(arangoDriver, existingCollections, bornInEdgeCollection, artistsCollectionName, locationsCollectionName, graphName);
        ArangoDAL.addEdgeCollectionIfNotExists(arangoDriver, existingCollections, deathInEdgeCollection, artistsCollectionName, locationsCollectionName, graphName);
        ArangoDAL.addEdgeCollectionIfNotExists(arangoDriver, existingCollections, memberOfEdgeCollection, artistsCollectionName, artMovementCollectionName, graphName);
        ArangoDAL.addEdgeCollectionIfNotExists(arangoDriver, existingCollections, describedByEdgeCollection, artistsCollectionName, artFieldCollectionName, graphName);
        ArangoDAL.addEdgeCollectionIfNotExists(arangoDriver, existingCollections, createdByEdgeCollection, artworkCollectionName, artistsCollectionName, graphName);
    }

}
