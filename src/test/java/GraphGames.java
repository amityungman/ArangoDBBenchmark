import com.arangodb.*;
import com.arangodb.entity.*;
import graphItems.*;
import com.arangodb.util.AqlQueryOptions;
import com.arangodb.util.MapBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


/**
 * Created by Amit on 10/10/2016.
 */
public class GraphGames {
    public static void main(String[] args) throws IOException {
        //Set up adapter
        ArangoConfigure configure = new ArangoConfigure();
        configure.init();
        configure.setUser("root");
        configure.setPassword("");
        configure.setArangoHost(new ArangoHost("127.0.0.1",8529));
        configure.init();
        ArangoDriver arangoDriver = new ArangoDriver(configure);
        arangoDriver.setDefaultDatabase("mydb");

        String graphName = "Academical";

        try {
            arangoDriver.deleteGraph(graphName,true);
            System.out.println("Graph deleted successfully");
        } catch (ArangoException e) {
            System.out.println("Failed to delete graph; " + e.getMessage());
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            br.readLine();
        }

        //Create graph
        // Edge definitions of the graph
        List<EdgeDefinitionEntity> edgeDefinitions = new ArrayList<EdgeDefinitionEntity>();

        // We start with one edge definition:
        EdgeDefinitionEntity edgeDefHasWritten = new EdgeDefinitionEntity();

        // Define the edge collection...
        edgeDefHasWritten.setCollection("HasWritten");

        // ... and the vertex collection(s) where an edge starts...
        List<String> from = new ArrayList<String>();
        from.add("Person");
        edgeDefHasWritten.setFrom(from);

        // ... and ends.
        List<String> to = new ArrayList<String>();
        to.add("Publication");
        edgeDefHasWritten.setTo(to);

        // add the edge definition to the list
        edgeDefinitions.add(edgeDefHasWritten);

        // We do not need any orphan collections, so this is just an empty list
        List<String> orphanCollections = new ArrayList<String>();

        // Create the graph:
        GraphEntity graphAcademical = null;
        try {
            graphAcademical = arangoDriver.createGraph(graphName, edgeDefinitions, orphanCollections, true);
        } catch (ArangoException e) {
            System.out.println("Failed to create graph; " + e.getMessage());
        }


        //Add another edge definition
        EdgeDefinitionEntity edgeDefHasCited = new EdgeDefinitionEntity();
        edgeDefHasCited.setCollection("HasCited");
        from.clear();
        from.add("Publication");
        edgeDefHasCited.setFrom(from);
        to.clear();
        to.add("Publication");
        edgeDefHasCited.setTo(to);

        // add the new definition to the existing graph:
        try {
            arangoDriver.graphCreateEdgeDefinition(graphName, edgeDefHasCited);
        } catch (ArangoException e) {
            System.out.println("Failed to add edge definition 'HasCited' to graph; " + e.getMessage());
        }

        //Add vertices
        DocumentEntity<Person> person1 = null;
        DocumentEntity<Person> person2 = null;
        DocumentEntity<Publication> publication1 = null;
        DocumentEntity<Publication> publication2 = null;
        DocumentEntity<Publication> publication3 = null;
        try {
            person1 = arangoDriver.graphCreateVertex(graphName, "Person", new Person("Bob", "Dr"), true);
            person2 = arangoDriver.graphCreateVertex(graphName, "Person", new Person("Floyd", "master of arts"), true);
            publication1 = arangoDriver.graphCreateVertex(graphName, "Publication", new Publication("Surgery for dummies", "1-234-1", 42), true);
            publication2 = arangoDriver.graphCreateVertex(graphName, "Publication", new Publication("Relaxing while in and working", "5-678-x", 815), true);
            publication3 = arangoDriver.graphCreateVertex(graphName, "Publication", new Publication("Infrasound in art and science", "7-081-5", 60), true);
        } catch (ArangoException e) {
            System.out.println("Failed to add vertices; " + e.getMessage());
        }

        //Add edges
        try {
            EdgeEntity edge = arangoDriver.graphCreateEdge(graphName, "HasWritten", null, person1.getDocumentHandle(), publication1.getDocumentHandle());
            String documentKey = edge.getDocumentKey();
            DocumentEntity<BaseDocument> documentEntity = arangoDriver.getDocument("HasWritten",documentKey, BaseDocument.class);
            BaseDocument baseDocument = documentEntity.getEntity();
            baseDocument.addAttribute("press_date","2015-10-15");
            arangoDriver.updateDocument(documentEntity.getDocumentHandle(),baseDocument);
            arangoDriver.graphCreateEdge(graphName, "HasWritten", null, person2.getDocumentHandle(), publication2.getDocumentHandle());
            arangoDriver.graphCreateEdge(graphName, "HasWritten", null, person2.getDocumentHandle(), publication3.getDocumentHandle());
            arangoDriver.graphCreateEdge(graphName, "HasCited", null, publication1.getDocumentHandle(), publication3.getDocumentHandle());
            arangoDriver.graphCreateEdge(graphName, "HasCited", null, publication1.getDocumentHandle(), publication2.getDocumentHandle());
        } catch (ArangoException e) {
            System.out.println("Failed to add vertices; " + e.getMessage());
        }

        //Select edges with AQL and short traversing
        String fromKey = "";
        try {
            String query = "FOR t IN HasWritten FILTER t.press_date == @date RETURN t";
            Map<String, Object> bindVars = new MapBuilder().put("date", "2015-10-15").get();
            DocumentCursor cursor = arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            Iterator iterator = cursor.entityIterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = (BaseDocument) iterator.next();
                fromKey = (String) aDocument.getAttribute("_from");
                Long fromId = Long.parseLong(fromKey.replace("Person/","")); //IMPORTANT - Notice the bug here - Key and ID are confused in the API
                DocumentEntity<Person> myDocument = arangoDriver.getDocument("Person", fromKey, Person.class);;
                Person myObject = myDocument.getEntity();
                System.out.println("Key: " + aDocument.getDocumentKey() + " | Written By: " + myObject.name);
            }
        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
            System.exit(0);
        }

        //Graph querying
        String query = "FOR v, e, p IN 1..2 OUTBOUND @personId GRAPH 'Academical' RETURN e";
        AqlQueryOptions aqlQueryOptions = new AqlQueryOptions();
        aqlQueryOptions.setFullCount(true);
        //aqlQueryOptions.setBatchSize(10);
        DocumentCursor<BaseDocument> graphQueryResult = null;
        try {
            Map<String, Object> bindVars = new MapBuilder().put("personId", fromKey).get();
            graphQueryResult = arangoDriver.executeDocumentQuery(query, bindVars, aqlQueryOptions, BaseDocument.class);

            Iterator iterator = graphQueryResult.entityIterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = (BaseDocument) iterator.next();
                System.out.println("Doc: " +  aDocument.getAttribute("_from"));
            }
        } catch (ArangoException e) {
            System.out.println("Failed to query graph. " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("This graph has " + graphQueryResult.getCount() + " edges.");
    }
}
