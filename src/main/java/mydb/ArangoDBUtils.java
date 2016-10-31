package mydb;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.ArangoHost;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;

import java.util.HashMap;

/**
 * Created by Amit on 10/10/2016.
 */
public class ArangoDBUtils {
    private ArangoDriver _arangoDriver = null;

    public ArangoDBUtils(ArangoDriver arangoDriver) {
        _arangoDriver = arangoDriver;
    }

    public ArangoDBUtils(String hostIP, int hostPort, String username, String password) {
        ArangoConfigure configure = new ArangoConfigure();
        //"http://127.0.0.1:8529"
        configure.init();
        configure.setUser(username);
        configure.setPassword(password);
        configure.setArangoHost(new ArangoHost(hostIP,hostPort));
        ArangoDriver arangoDriver = new ArangoDriver(configure);

        _arangoDriver = arangoDriver;
    }

    public void CreateNewDB(String dbName) throws ArangoException {
        _arangoDriver.createDatabase(dbName);
    }

    public CollectionEntity CreateNewCollection(String collectionName) throws ArangoException {
        return _arangoDriver.createCollection(collectionName);
    }

    public void setDefaultDatabase(String defaultDatabase) {
        _arangoDriver.setDefaultDatabase(defaultDatabase);
    }

    public void createDocument(String collectionName, String documentKey, HashMap<String, Object> attributes) throws ArangoException {
        BaseDocument myObject = new BaseDocument();
        myObject.setDocumentKey(documentKey);
        for(String key : attributes.keySet())
            myObject.addAttribute(key,attributes.get(key));

        _arangoDriver.createDocument(collectionName, myObject);
    }
}
