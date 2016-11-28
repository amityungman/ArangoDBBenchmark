package utils;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * Created by Amit on 28/11/2016.
 */
public class CollectionUtils {
    public static <T> boolean has(Collection<T> collection, Predicate predicate) {
        for(T elm : collection) {
            if(predicate.test(elm))
                return true;
        }
        return false;
    }
}
