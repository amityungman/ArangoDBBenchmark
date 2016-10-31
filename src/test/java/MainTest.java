import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.DocumentEntity;
import com.arangodb.entity.GraphEntity;
import com.arangodb.util.MapBuilder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;


/**
 * Created by Amit on 10/10/2016.
 */
public class MainTest {
    public static void main(String[] args) {
        ArangoConfigure configure = new ArangoConfigure();
        configure.init();
        configure.setUser("root");
        configure.setPassword("");
        configure.setArangoHost(new ArangoHost("127.0.0.1",8529));
        configure.init();
        ArangoDriver arangoDriver = new ArangoDriver(configure);

        //Create new DB
        String dbName = "mydb";
        try {
            arangoDriver.createDatabase(dbName);
            System.out.println("Database created: " + dbName);
        } catch (Exception e) {
            System.out.println("Failed to create database " + dbName + "; " + e.getMessage());
        }

        //Set default DB
        arangoDriver.setDefaultDatabase(dbName);

        //Create new collection
        String collectionName = "firstCollection";
        try {
            CollectionEntity myArangoCollection = arangoDriver.createCollection(collectionName);
            System.out.println("Collection created: " + myArangoCollection.getName());
        } catch (Exception e) {
            System.out.println("Failed to create colleciton " + collectionName + "; " + e.getMessage());
        }

        //Create new document
        BaseDocument myObject = new BaseDocument();
        myObject.setDocumentKey("myKey");
        myObject.addAttribute("a", "Foo");
        myObject.addAttribute("b", 42);
        try {
            arangoDriver.createDocument(collectionName, myObject);
            System.out.println("Document created");
        } catch (ArangoException e) {
            System.out.println("Failed to create document. " + e.getMessage());
        }

        //Read document
        DocumentEntity<BaseDocument> myDocument = null;
        BaseDocument myObject2 = null;
        try {
            myDocument = arangoDriver.getDocument(collectionName, "myKey", BaseDocument.class);
            myObject2 = myDocument.getEntity();
            System.out.println("Key: " + myObject2.getDocumentKey());
            System.out.println("Attribute 'a': " + myObject2.getProperties().get("a"));
            System.out.println("Attribute 'b': " + myObject2.getProperties().get("b"));
            System.out.println("Attribute 'c': " + myObject2.getProperties().get("c"));
        } catch (ArangoException e) {
            System.out.println("Failed to get document. " + e.getMessage());
        }

        //Update a document
        try {
            myObject2.addAttribute("c", "Bar");
            arangoDriver.updateDocument(myDocument.getDocumentHandle(), myObject2);
        } catch (ArangoException e) {
            System.out.println("Failed to update document. " + e.getMessage());
        }

        //Reread the document
        try {
            myDocument = arangoDriver.getDocument(collectionName, "myKey", BaseDocument.class);
            myObject2 = myDocument.getEntity();
            System.out.println("Key: " + myObject2.getDocumentKey());
            System.out.println("Attribute 'a': " + myObject2.getProperties().get("a"));
            System.out.println("Attribute 'b': " + myObject2.getProperties().get("b"));
            System.out.println("Attribute 'c': " + myObject2.getProperties().get("c"));
        } catch (ArangoException e) {
            System.out.println("Failed to get document. " + e.getMessage());
        }

        //Delete a document
        try {
            arangoDriver.deleteDocument(myDocument.getDocumentHandle());
        } catch (ArangoException e) {
            System.out.println("Failed to delete document. " + e.getMessage());
        }

        //Recreate a document
        myObject = new BaseDocument();
        myObject.setDocumentKey("myKey");
        myObject.addAttribute("a", "Foo");
        myObject.addAttribute("b", 42);
        try {
            arangoDriver.createDocument(collectionName, myObject);
            System.out.println("Document created");
        } catch (ArangoException e) {
            System.out.println("Failed to create document. " + e.getMessage());
        }

        //Read document
        try {
            myDocument = arangoDriver.getDocument(collectionName, "myKey", BaseDocument.class);
            myObject2 = myDocument.getEntity();
            System.out.println("Key: " + myObject2.getDocumentKey());
            System.out.println("Attribute 'a': " + myObject2.getProperties().get("a"));
            System.out.println("Attribute 'b': " + myObject2.getProperties().get("b"));
            System.out.println("Attribute 'c': " + myObject2.getProperties().get("c"));
        } catch (ArangoException e) {
            System.out.println("Failed to get document. " + e.getMessage());
        }

        System.out.println("\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");

        //Test AQL
        GenerateData(arangoDriver, collectionName, 20);

        //Select with AQL
        try {
            String query = "FOR t IN firstCollection FILTER t.name == @name RETURN t";
            Map<String, Object> bindVars = new MapBuilder().put("name", 1).get();
            DocumentCursor cursor = arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            Iterator iterator = cursor.entityIterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = (BaseDocument) iterator.next();
                System.out.println("Key: " + aDocument.getDocumentKey());
            }
        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
        }


        //Delete with AQL
        try {
            String query = "FOR t IN firstCollection FILTER t.name == @name "
                    + "REMOVE t IN firstCollection LET removed = OLD RETURN removed";
            Map<String, Object> bindVars = new MapBuilder().put("name", "Homer").get();
            DocumentCursor rs = arangoDriver.executeDocumentQuery(query, bindVars, null,
                    BaseDocument.class);

            Iterator<BaseDocument> iterator = rs.iterator();
            while (iterator.hasNext()) {
                BaseDocument aDocument = iterator.next();
                System.out.println("Removed document: " + aDocument.getDocumentKey());
            }

        } catch (ArangoException e) {
            System.out.println("Failed to execute query. " + e.getMessage());
        }
    }


    private static void GenerateData(ArangoDriver arangoDriver, String collectionName, int amount) {

        for(int i = 0 ; i < amount ; i++) {
            BaseDocument myObject = new BaseDocument();
            myObject.setDocumentKey(i + "");
            myObject.addAttribute("name", new Random().nextInt(10));
            myObject.addAttribute("excerpt", JoinStrings(" ",getRandomStrings(6,20)));
            myObject.addAttribute("content", JoinStrings(" ",getRandomStrings(6,100)));
            try {
                arangoDriver.createDocument(collectionName, myObject);
                System.out.println("Document " + i + " created");
            } catch (ArangoException e) {
                System.out.println("Failed to create document. " + e.getMessage());
            }
        }
    }

    public static String[] getRandomStrings(final int characterLength, final int generateSize) {
        HashSet<String> list = new HashSet<String>();
        for (int i = 0; i < generateSize; ++i) {
            String name = null;
            list.add(org.apache.commons.lang.RandomStringUtils.randomAlphanumeric(
                    org.apache.commons.lang.math.RandomUtils.nextInt(characterLength - 1) + 1));
        }

        return list.toArray(new String[]{});
    }

    public static String JoinStrings(String delimiter, String[] strings) {
        StringBuilder sb = new StringBuilder();

        for(String str : strings)
            sb = sb.append(str).append(delimiter);

        return sb.toString();
    }
}
