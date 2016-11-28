package benchmark;

import graphItems.ArtField;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.DocumentCursor;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.util.MapBuilder;
import sparql.dbpediaObjects.DBPediaArtist;
import utils.LambdaTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static benchmark.ArtistsTest.*;

/**
 * Created by Amit on 31/10/2016.
 */
public class ReadWriteDeleteUpdateTest {
    public static final String NOT_IMPLEMENTED = "\u001B[31mNOT IMPLEMENTED | \u001B[0m" ;
    private static boolean isRebuildMode = true;
    private static long _timer = 0;

    private static ArangoDriver _arangoDriver;

    public static void main(String[] args) throws IOException, ArangoException {
        double passedSeconds = getPassedSeconds();
        //1.1
        _arangoDriver = ArtistsTest.setUpDBPediaArtistsDB(isRebuildMode,true);
        if(isRebuildMode)
            System.out.format("\u001B[33m1.1\u001B[0m - Created artists graph \u001B[32m[%.6f seconds]\u001B[0m.\n", getPassedSeconds());
        getPassedSeconds();

        //RESET data
        if(!isRebuildMode)
            resetDoneActions();

        //1.2
        passedSeconds = updateAnArtworkName("The Starry Night", "Starry Night");
        System.out.format("\u001B[33m1.2\u001B[0m - Updated single artwork name \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //1.3
        passedSeconds = createAndConnectTakingPhotos();
        System.out.format("\u001B[33m1.3\u001B[0m - Created \"Taking Photos\" art movement node, and connect all photography related to it \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //1.4
        passedSeconds = deletePhotographyMovement();
        System.out.format("\u001B[33m1.4\u001B[0m - Deleted \"Photography\" and all relations \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //1.5
        passedSeconds = updateEdgesWeight(ArtistsTest.bornInEdgeCollection, 1.0);
        System.out.format("\u001B[33m1.5\u001B[0m - Update BORN_IN edges to have weight=1 \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //1.6
        passedSeconds = deleteAllParisianPainters();
        System.out.format("\u001B[33m1.6\u001B[0m - Deleted all painters born in Paris \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //1.7
        passedSeconds = updateEdgesWeight(ArtistsTest.deathInEdgeCollection, 0.5);
        System.out.format("\u001B[33m1.7\u001B[0m - Update all DIED_IN edges to have weight=0.5 \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);


        //3.1
        passedSeconds = searchForName("Vincent van Gogh", false);
        System.out.format("\u001B[33m3.1\u001B[0m - Search for Vincent van Gogh \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //3.2
        passedSeconds = searchForName("Van Gogh", true);
        System.out.format("\u001B[33m3.2\u001B[0m - Fuzzy search for Van Gogh \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //3.3
        passedSeconds = searchForDiedAfterDate("1950-01-01");
        System.out.format("\u001B[33m3.3\u001B[0m - Search for people who died after 1950 \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //3.4
        passedSeconds = searchInDescription("son");
        System.out.format("\u001B[33m3.4\u001B[0m - Search for people who have \"son\" in their description \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);


        //4.1
        passedSeconds = findShortestPath("Pablo Picasso", "France");
        System.out.format("\u001B[33m4.1\u001B[0m - Find shortest path between Picasso and France \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //4.2
        passedSeconds = findShortestPathNotUsingArtMovementOrField("Pablo Picasso", "France");
        System.out.format("\u001B[33m4.2\u001B[0m - Find if there are paths from Picasso to France, without going through an Art movement node \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);

        //4.3
        passedSeconds = findLightestPath("Pablo Picasso", "France");
        System.out.format("\u001B[33m4.3\u001B[0m - Find lightest path from Picasso to France \u001B[32m[%.6f seconds]\u001B[0m.\n", passedSeconds);
    }

    private static void resetDoneActions() throws ArangoException {
        String query = "FOR t IN Artworks FILTER LOWER(t.Name) ==  @artworkName UPDATE {  _key: t._key, Name: @newName } IN Artworks";
        Map<String, Object> bindVars = new MapBuilder().put("artworkName", "starry night").put("newName", "The Starry Night").get();
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);


        DocumentEntity<ArtField> artField = getOrCreateVertex(_arangoDriver, graphName,
                artFieldCollectionName, "PHOTOGRAPHY", new ArtField("PHOTOGRAPHY"), ArtField.class);

        query = "FOR v, e, p \n" +
                "IN 1..2 \n" +
                "INBOUND 'ArtFields/PHOTO_TAKING'\n" +
                "  DESCRIBED_BY\n" +
                "RETURN p.vertices[1]";
        bindVars = new MapBuilder().get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            _arangoDriver.graphCreateEdge(graphName, describedByEdgeCollection, null, entity.getDocumentHandle(), artField.getDocumentHandle());
        }

        //Delete edges
        query = "FOR v, e, p \n" +
                "IN 1..2\n" +
                "INBOUND 'ArtFields/PHOTO_TAKING' \n" +
                "DESCRIBED_BY \n" +
                "REMOVE { _key: p.edges[0]._key } IN DESCRIBED_BY";
        bindVars = new MapBuilder().get();
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);

        //Delete PHOTOGRAPHY
        query = "FOR v IN ArtFields\n" +
                "FILTER v.Name == 'PHOTO_TAKING'\n" +
                "REMOVE { _key: v._key } IN ArtFields";
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);

        ArtistsTest.addAllNodes(_arangoDriver, new LambdaTest() {
            @Override
            public <T> boolean check(T value) {
                DBPediaArtist artist = (DBPediaArtist) value;
                for(String location : artist.getBirthPlace())
                    if(location.toLowerCase().equals("paris"))
                        return true;
                return false;
            }
        });
    }

    private static double updateAnArtworkName(String originalName, String newName) throws ArangoException {
        double secondsPassed;

        //String query = "FOR t IN Artists FILTER t.Name == @artistName UPDATE t.Name: @newName";
        String query = "FOR t IN Artworks FILTER LOWER(t.Name) ==  @artworkName RETURN t";
        Map<String, Object> bindVars = new MapBuilder().put("artworkName", originalName.toLowerCase()).get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            System.out.println("Found name in document key: " + entity.getDocumentKey());
        }

        getPassedSeconds();
        query = "FOR t IN Artworks FILTER LOWER(t.Name) ==  @artworkName UPDATE {  _key: t._key, Name: @newName } IN Artworks";
        bindVars = new MapBuilder().put("artworkName", originalName.toLowerCase()).put("newName", newName).get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        secondsPassed = getPassedSeconds();

        query = "FOR t IN Artworks FILTER LOWER(t.Name) ==  @artworkName RETURN t";
        bindVars = new MapBuilder().put("artworkName", newName.toLowerCase()).get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            System.out.println("Updated successfully document key: " + entity.getDocumentKey());
        }

        return secondsPassed;
    }

    private static double createAndConnectTakingPhotos() throws ArangoException {
        getPassedSeconds();

        DocumentEntity<ArtField> artField = getOrCreateVertex(_arangoDriver, graphName,
                artFieldCollectionName, "PHOTO_TAKING", new ArtField("PHOTO_TAKING"), ArtField.class);

        String query = "FOR v, e, p \n" +
                "IN 1..2 \n" +
                "INBOUND 'ArtFields/PHOTOGRAPHY'\n" +
                "  DESCRIBED_BY\n" +
                "RETURN p.vertices[1]";
        Map<String, Object> bindVars = new MapBuilder().get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            _arangoDriver.graphCreateEdge(graphName, describedByEdgeCollection, null, entity.getDocumentHandle(), artField.getDocumentHandle());
        }

        updateEdgesWeight("BORN_IN", -1);
        updateEdgesWeight("DIED_IN", -1);

        return getPassedSeconds();
    }

    private static double deletePhotographyMovement() throws ArangoException {
        getPassedSeconds();

        //Delete edges
        String query = "FOR v, e, p \n" +
                "IN 1..2\n" +
                "INBOUND 'ArtFields/PHOTOGRAPHY' \n" +
                "DESCRIBED_BY \n" +
                "REMOVE { _key: p.edges[0]._key } IN DESCRIBED_BY";
        Map<String, Object> bindVars = new MapBuilder().get();
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);

        //Delete PHOTOGRAPHY
        query = "FOR v IN ArtFields\n" +
                "FILTER v.Name == 'PHOTOGRAPHY'\n" +
                "REMOVE { _key: v._key } IN ArtFields";
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);


        return getPassedSeconds();
    }

    private static double updateEdgesWeight(String edgeTypeName, double weight) throws ArangoException {
        getPassedSeconds();

        String query = "FOR v IN " + edgeTypeName.toUpperCase() + "\n" +
                "UPDATE { _key: v._key, Weight: @newWeight } IN " + edgeTypeName.toUpperCase();
        Map<String, Object> bindVars = new MapBuilder().put("newWeight",weight).get();
        _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);

        return getPassedSeconds();
    }

    private static double deleteAllParisianPainters() throws ArangoException {
        getPassedSeconds();

        //Get artists
        String query = "FOR v, e, p IN 1..2 INBOUND 'Locations/PARIS'\n" +
                "BORN_IN\n" +
                "RETURN p.vertices[1]";
        Map<String, Object> bindVars = new MapBuilder().get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            query = "FOR v, e, p IN 1..2 INBOUND @artistID\n" +
                    "BORN_IN\n" +
                    "REMOVE { _key: p.edges[0]._key} IN BORN_IN\n";
            bindVars = new MapBuilder().put("artistID",entity.getDocumentKey()).get();
            _arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            query = "FOR v, e, p IN 1..2 INBOUND @artistID\n" +
                    "DIED_IN\n" +
                    "REMOVE { _key: p.edges[0]._key} IN DIED_IN\n";
            bindVars = new MapBuilder().put("artistID",entity.getDocumentKey()).get();
            _arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            query = "FOR v, e, p IN 1..2 INBOUND @artistID\n" +
                    "MEMBER_OF\n" +
                    "REMOVE { _key: p.edges[0]._key} IN MEMBER_OF\n";
            bindVars = new MapBuilder().put("artistID",entity.getDocumentKey()).get();
            _arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            query = "FOR v, e, p IN 1..2 INBOUND @artistID\n" +
                    "DESCRIBED_BY\n" +
                    "REMOVE { _key: p.edges[0]._key} IN DESCRIBED_BY\n";
            bindVars = new MapBuilder().put("artistID",entity.getDocumentKey()).get();
            _arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);
        }

        return getPassedSeconds();
    }

    private static double searchForName(String name, boolean isFuzzySearch) throws ArangoException {
        getPassedSeconds();

        String query = "";
        if(isFuzzySearch)
            query = "FOR a IN FULLTEXT('Artists','Description','complete:" + name + "')\n" +
                "RETURN a";
        else
            query = "FOR a IN Artists\n" +
                "FILTER contains(a.Description,'" + name + "')\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            System.out.println("Found " + name + " in document key: " + entity.getDocumentKey());
        }

        return getPassedSeconds();
    }

    private static double searchForDiedAfterDate(String deathDate) throws ArangoException {
        getPassedSeconds();

        String query = "FOR a IN Artists\n" +
                "FILTER a.DeathDate >= @date\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().put("date",deathDate).get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        int counter = 0;
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            counter++;
        }
        System.out.println("Found " + counter + " deaths after " + deathDate);

        return getPassedSeconds();
    }

    private static double searchInDescription(String searchWord) throws ArangoException {
        getPassedSeconds();

        String query = "FOR a IN FULLTEXT('Artists','Description','complete:" + searchWord + "')\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        int counter = 0;
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            counter++;
        }
        System.out.println("Found " + counter + " with " + searchWord + " in the description");

        return getPassedSeconds();
    }

    private static double findShortestPath(String firstNode, String secondNode) throws ArangoException {
        getPassedSeconds();
        String firstKey = "", secondKey = "";

        String query = "FOR a IN Artists\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().put("name",firstNode).get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        int counter = 0;
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            firstKey = entity.getDocumentKey();
        }

        query = "FOR a IN Locations\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        bindVars = new MapBuilder().put("name",secondNode).get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            secondKey = entity.getDocumentKey();
        }

        query = "FOR v, e IN ANY SHORTEST_PATH\n" +
                "  'Artists/" +  firstKey + "' TO 'Locations/" +  secondKey + "'\n" +
                "  GRAPH 'Art'\n" +
                "  RETURN v";

        bindVars = new MapBuilder().get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        System.out.println("Connection between Picasso and France: ");
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            System.out.print(entity.getAttribute("Name") + "-");
        }
        System.out.println();

        return getPassedSeconds();
    }

    private static double findShortestPathNotUsingArtMovementOrField(String firstNode, String secondNode) throws ArangoException {
        getPassedSeconds();
        String firstKey = "", secondKey = "";

        String query = "FOR a IN Artists\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().put("name",firstNode).get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            firstKey = entity.getDocumentKey();
        }

        query = "FOR a IN Locations\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        bindVars = new MapBuilder().put("name",secondNode).get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            secondKey = entity.getDocumentKey();
        }

        query = "FOR v, e IN ANY SHORTEST_PATH\n" +
                "  'Artists/" +  firstKey + "' TO 'Locations/" +  secondKey + "'\n" +
                "  DIED_IN, BORN_IN, CREATED_BY\n" +
                "  RETURN v";

        bindVars = new MapBuilder().get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        System.out.println("Connection between Picasso and France: ");
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            System.out.print(entity.getAttribute("Name") + "-");
        }
        System.out.println();

        return getPassedSeconds();
    }

    private static double findLightestPath(String firstNode, String secondNode) throws ArangoException {
        getPassedSeconds();
        String firstKey = "", secondKey = "";

        String query = "FOR a IN Artists\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        Map<String, Object> bindVars = new MapBuilder().put("name",firstNode).get();
        DocumentCursor cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        Iterator entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            firstKey = entity.getDocumentKey();
        }

        query = "FOR a IN Locations\n" +
                "FILTER a.Name == @name\n" +
                "RETURN a";

        bindVars = new MapBuilder().put("name",secondNode).get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        while(entityIterator.hasNext()) {
            BaseDocument entity = (BaseDocument) entityIterator.next();
            secondKey = entity.getDocumentKey();
        }

        query = "FOR v, e, p IN 1..4 ANY \n" +
                "'Artists/" +  firstKey + "'\n" +
                "GRAPH 'Art'\n" +
                "FILTER p.vertices[3]._id == 'Locations/" +  secondKey + "'\n" +
                "RETURN p";

        bindVars = new MapBuilder().get();
        cursor = _arangoDriver.executeDocumentQuery(query, bindVars, null,
                BaseDocument.class);
        entityIterator = cursor.entityIterator();
        BaseDocument lightestPath = null;
        double lightestWeight = 999999;
        while(entityIterator.hasNext()) {
            double currentWeight = 0;
            BaseDocument entity = (BaseDocument) entityIterator.next();
            ArrayList edges = (ArrayList) entity.getAttribute("edges");
            for(Object mapObject : edges) {
                HashMap map = (HashMap) mapObject;
                Object weight = map.get("Weight");
                if(weight != null)
                    currentWeight += (Double) weight;
            }

            if(currentWeight < lightestWeight) {
                lightestPath = entity;
                lightestWeight = currentWeight;
            }
        }

        System.out.println("Lightest connection between Picasso and France: ");
        ArrayList vertices = (ArrayList) lightestPath.getAttribute("vertices");
        for(Object mapObject : vertices) {
            HashMap map = (HashMap) mapObject;
            System.out.print(map.get("Name") + "-");
        }

        System.out.println();

        return getPassedSeconds();
    }


    /***
     * A timer method
     * @return The number of seconds passed since last call
     */
    private static double getPassedSeconds() {
        double secondsPassed = (System.currentTimeMillis() - _timer)/1000.0;
        _timer = System.currentTimeMillis();
        return secondsPassed;
    }
}
