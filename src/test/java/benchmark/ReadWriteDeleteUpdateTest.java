package benchmark;

import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;

import java.io.IOException;

/**
 * Created by Amit on 31/10/2016.
 */
public class ReadWriteDeleteUpdateTest {
    private static boolean isRebuildMode = false;
    private static long _timer = 0;

    public static void main(String[] args) throws IOException, ArangoException {
        double passedSeconds = getPassedSeconds();
        //1.1
        ArangoDriver arangoDriver = ArtistsTest.setUpDBPediaArtistsDB(isRebuildMode,true);
        if(isRebuildMode)
            System.out.format("1.1 - Created artists graph [%.4f seconds].\n", getPassedSeconds());
        getPassedSeconds();

        //1.2
        updateAnArtworkName("The Starry Night", "Starry Night");
        System.out.format("1.2 - Updated single artwork name [%.4f seconds].\n", getPassedSeconds());

        //1.3
        createAndConnectTakingPhotos();
        System.out.format("1.3 - Created \"Taking Photos\" art movement node, and connect all photography related to it [%.4f seconds].\n", getPassedSeconds());

        //1.4
        deletePhotographyMovement();
        System.out.format("1.4 - Deleted \"Photography\" and all relations [%.4f seconds].\n", getPassedSeconds());

        //1.5
        updateEdgesWeight(ArtistsTest.bornInEdgeCollection, 1.0);
        System.out.format("1.5 - Update BORN_IN edges to have weight=1 [%.4f seconds].\n", getPassedSeconds());

        //1.6
        deleteAllParisianPainters();
        System.out.format("1.6 - Deleted all painters born in Paris [%.4f seconds].\n", getPassedSeconds());

        //1.7
        updateEdgesWeight(ArtistsTest.deathInEdgeCollection, 0.5);
        System.out.format("1.7 - Update all DIED_IN edges to have weight=0.5 [%.4f seconds].\n", getPassedSeconds());


        //3.1
        searchForName("Vincent van Gogh", false);
        System.out.format("3.1 - Search for Vincent van Gogh [%.4f seconds].\n", getPassedSeconds());

        //3.2
        searchForName("Van Gogh", true);
        System.out.format("3.2 - Fuzzy search for Van Gogh [%.4f seconds].\n", getPassedSeconds());

        //3.3
        searchForDiedAfterDate("1950-1-1");
        System.out.format("3.3 - Search for people who died after 1950 [%.4f seconds].\n", getPassedSeconds());

        //3.4
        searchInDescription("son");
        System.out.format("3.4 - Search for people who have \"son\" in their description [%.4f seconds].\n", getPassedSeconds());


        //4.1
        findShortestPath("Picasso", "France");
        System.out.format("4.1 - Find shortest path between Picasso and France [%.4f seconds].\n", getPassedSeconds());

        //4.2
        findShortestPathNotUsingArtMovement("Picasso", "France");
        System.out.format("4.2 - Find if there are paths from Picasso to France, without going through an Art movement node [%.4f seconds].\n", getPassedSeconds());

        //4.3
        findLightestPath("Picasso", "France");
        System.out.format("4.3 - Find lightest path from Picasso to France [%.4f seconds].\n", getPassedSeconds());
    }

    private static void updateAnArtworkName(String originalName, String newName) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void createAndConnectTakingPhotos() {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void deletePhotographyMovement() {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void updateEdgesWeight(String edgeTypeName, double weight) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void deleteAllParisianPainters() {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void searchForName(String name, boolean isFuzzySearch) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void searchForDiedAfterDate(String deathDate) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void searchInDescription(String searchWord) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void findShortestPath(String firstNode, String secondNode) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void findShortestPathNotUsingArtMovement(String firstNode, String secondNode) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
    }

    private static void findLightestPath(String firstNode, String secondNode) {
        //TODO implement
        System.out.print("NOT IMPLEMENTED");
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
