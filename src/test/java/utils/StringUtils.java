package utils;

import java.text.Normalizer;

/**
 * Created by Amit on 28/11/2016.
 */
public class StringUtils {
    public static String cleanText(String key) {
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
}
