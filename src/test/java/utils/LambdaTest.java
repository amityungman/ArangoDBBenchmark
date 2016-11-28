package utils;

import org.apache.jena.atlas.iterator.Action;
import sparql.dbpediaObjects.DBPediaArtist;

/**
 * Created by Amit on 01/11/2016.
 */
public abstract class LambdaTest {
    public abstract <T> boolean check(T value);
}
