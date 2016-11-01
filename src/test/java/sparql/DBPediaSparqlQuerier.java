package sparql;

import javafx.util.Pair;
import sparql.dbpediaObjects.ARTWORK_TYPE;
import sparql.dbpediaObjects.DBPediaArtist;
import sparql.dbpediaObjects.DBPediaArtwork;
import sparql.dbpediaObjects.DBPediaPerson;

import java.util.*;

/**
 * Queries over DBPedia with sparql.
 * Created by Amit on 18/10/2016.
 */
public class DBPediaSparqlQuerier {

    /***
     * Gets the most famous artists in the DBPedia database. NOT MUSICIANS!
     * @param maxAmount - The maximal amount of artists wanted
     * @return A list of the most famous artists and properties about them -
     * name, description, field[] (in art), movement[] (in art), birthDate, deathDate, birthPlace[], deathPlace[], totalLinks (external links in WIKI), totalArt (artwork listed in WIKI)
     */
    public static List<DBPediaArtist> getMostFamousArtists(int maxAmount) {
        List<DBPediaArtist> artists = new LinkedList<DBPediaArtist>();
        /*String query =
                "SELECT ?id ?name ?description (group_concat(distinct ?aField ; separator = \";\") AS ?field) (group_concat(distinct ?aMovement ; separator = \";\") AS ?movement) ?birthDate ?deathDate (group_concat(distinct ?aBirthPlace ; separator = \";\") AS ?birthPlace)  (group_concat(distinct ?aDeathPlace ; separator = \";\") AS ?deathPlace) (COUNT(DISTINCT ?links) AS ?totalLinks) (COUNT(DISTINCT ?art) AS ?totalArt) WHERE {\n" +
                        "      ?person a dbo:Artist .\n" +
                        "      ?person rdfs:comment ?description .\n" +
                        "      ?person dbo:wikiPageID ?id .\n" +
                        "      OPTIONAL {?person dbo:field ?aField} .\n" +
                        "      OPTIONAL {?person dbo:movement ?aMovement} .\n" +
                        "      ?person dbo:birthPlace ?aBirthPlace .\n" +
                        "      ?person dbo:birthDate ?birthDate .\n" +
                        "      OPTIONAL {?person dbo:deathPlace ?aDeathPlace} .\n" +
                        "      OPTIONAL {?person dbo:deathDate ?deathDate} .\n" +
                        "      ?person foaf:name ?name .\n" +
                        "      ?person dbo:wikiPageExternalLink ?links.\n" +
                        "      ?art    dbpedia2:artist ?person .\n" +
                        "      FILTER (LANG(?description) = 'en') . \n" +
                        "      FILTER NOT EXISTS {\n" +
                        "         ?person rdf:type <http://dbpedia.org/class/yago/Musician110339966> .\n" +
                        "      }\n" +
                        "      FILTER NOT EXISTS {\n" +
                        "         ?person rdf:type <http://dbpedia.org/ontology/MusicalArtist>\n" +
                        "      }\n" +
                        "}\n" +
                        "GROUP BY ?id ?name ?description ?birthDate ?deathDate\n" +
                        "HAVING (COUNT(DISTINCT ?art) >= 1)\n" +
                        "ORDER BY DESC(?totalArt)\n" +
                        "LIMIT " + maxAmount;*/
        String query =
                "SELECT ?id ?name ?description (group_concat(distinct ?aField ; separator = \";\") AS ?field) (group_concat(distinct ?aMovement ; separator = \";\") AS ?movement) ?birthDate ?deathDate (group_concat(distinct ?aBirthPlace ; separator = \";\") AS ?birthPlace)  (group_concat(distinct ?aDeathPlace ; separator = \";\") AS ?deathPlace) (COUNT(DISTINCT ?links) AS ?totalLinks) (COUNT(DISTINCT ?art) AS ?totalArt) WHERE {\n" +
                        "      ?person a dbo:Artist .\n" +
                        "      ?person rdfs:comment ?description .\n" +
                        "      ?person dbo:wikiPageID ?id .\n" +
                        "      OPTIONAL {?person dbo:field ?aField} .\n" +
                        "      OPTIONAL {?person dbo:movement ?aMovement} .\n" +
                        "      ?person dbo:birthPlace ?aBirthPlace .\n" +
                        "      ?person dbo:birthDate ?birthDate .\n" +
                        "      OPTIONAL {?person dbo:deathPlace ?aDeathPlace} .\n" +
                        "      OPTIONAL {?person dbo:deathDate ?deathDate} .\n" +
                        "      ?person foaf:name ?name .\n" +
                        "      ?person dbo:wikiPageExternalLink ?links.\n" +
                        "      ?art    dbpedia2:artist ?person .\n" +
                        "      FILTER (LANG(?description) = 'en') . \n" +
                        "}\n" +
                        "GROUP BY ?id ?name ?description ?birthDate ?deathDate\n" +
                        "HAVING (COUNT(DISTINCT ?art) >= 1)\n" +
                        "ORDER BY DESC(?totalArt)\n" +
                        "LIMIT " + maxAmount;

        for(Map<String,String> artistProperties : SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA)) {
            DBPediaArtist artist = new DBPediaArtist(
                    artistProperties.get("id"),
                    artistProperties.get("name"),
                    artistProperties.get("description"),
                    artistProperties.get("field").split(";"),
                    artistProperties.get("movement").split(";"),
                    artistProperties.get("birthDate"),
                    artistProperties.get("deathDate"),
                    artistProperties.get("birthPlace").split(";"),
                    artistProperties.get("deathPlace").split(";"),
                    Integer.parseInt(artistProperties.get("totalLinks")),
                    Integer.parseInt(artistProperties.get("totalArt"))
            );
            artists.add(artist);
        }
        return artists;
    }

    /***
     * Get all wiki redirect terms for artists
     * @param artistNames - A list of the artists' names
     * @return A Map between the name of the artist, and a list of all its synonyms.
     */
    public static Map<String, List<String>> getArtistSysnonyms(List<String> artistNames) {
        String query = "SELECT ?person (group_concat(distinct ?aSynonym ; separator = \";\") AS ?synonym) WHERE {\n" +
                "      ?person a dbo:Artist .\n" +
                "      ?aSynonym dbo:wikiPageRedirects ?person .\n" +
                "      ?person foaf:name ?name .\n" +
                "      FILTER (:names:)\n" +
                "}\n" +
                "GROUP BY ?person";
        StringBuilder sb = new StringBuilder();
        for(int i = 0 ; i < artistNames.size() ; i++) {
            sb.append("(?name = \"" + artistNames.get(i) + "\"@en)");
            if(i != artistNames.size()-1)
                sb.append(" || ");
        }
        query = query.replace(":names:",sb.toString());

        Map<String, List<String>> synonyms = new HashMap<String, List<String>>();
        for(Map<String,String> result : SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA))
            synonyms.put(result.get("person"), new LinkedList<String>(Arrays.asList(result.get("synonym").split(";"))));


        return synonyms;
    }

    /***
     * Gets the artworks for an artist
     * @param artistNames - A list of the artists' names
     * @return A map between each artist and name and the artwork
     */
    public static Map<String, DBPediaArtwork[]> getAtristsArtwork(List<String> artistNames) {
        //TODO implement
        throw new RuntimeException("getArtistsArtwork not yet impelemented");
    }

    /***
     * Gets an artist's artwork
     * @param wikiPageID - The id of the artist
     * @return A List of the artist's artwork
     */
    public static List<DBPediaArtwork> getAtristArtwork(String wikiPageID) {
        List<DBPediaArtwork> artwork = new LinkedList<DBPediaArtwork>();

        String query = "SELECT ?id ?art ?title ?description (group_concat(distinct ?aSubject ; separator = \";\") AS ?subject) WHERE {\n" +
                "      ?person a dbo:Artist .\n" +
                "      ?art    dbo:abstract ?description .\n" +
                "      ?art    dbpedia2:artist ?person .\n" +
                "      ?art    dbo:wikiPageID ?id .\n" +
                "      OPTIONAL {?art    dbpedia2:title ?title} .\n" +
                "      OPTIONAL {?art    dct:subject  ?aSubject} .\n" +
                "      ?person dbo:wikiPageID :id: .\n" +
                "      FILTER (LANG(?description) = 'en') . \n" +
                "} GROUP BY ?id ?art ?title ?description";

        query = query.replace(":id:",wikiPageID);

        for(Map<String,String> result : SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA))
            artwork.add(new DBPediaArtwork(result.get("id"), result.get("art"), result.get("description"), getCreationYearFromSubjects(result.get("subject").split(";")), getArtworkTypeFromSubjects(result.get("subject").split(";"))));

        return artwork;
    }

    private static ARTWORK_TYPE getArtworkTypeFromSubjects(String[] subjects) {
        for(String subject : subjects) {
            for(String subjectPart : subject.replaceAll("\\d+","").split(" ")) {
                ARTWORK_TYPE artworkType = ARTWORK_TYPE.parse(subjectPart);
                if (artworkType.equals(ARTWORK_TYPE.OTHER))
                    continue;
                return artworkType;
            }
        }
        return ARTWORK_TYPE.OTHER;
    }

    /***
     * Extract the creation year from the DBPedia subjects
     * @param subjects - The list of subjects
     * @return The creation year if found. Otherwise, empty string
     */
    private static String getCreationYearFromSubjects(String[] subjects) {
        String year = "0";

        for(String subject : subjects) {
            if(subject.matches("Category:\\d\\d\\d\\ds paintings") || subject.matches("Category:\\d\\d\\d\\d paintings")) {
                String yearString = subject.substring(9,13);
                year = Integer.parseInt(yearString) > Integer.parseInt(year) ? yearString : year;
            }
        }

        return year.equals("0") ? "" : year;
    }

    /***
     * Checks the type of the location given with the DBPedia DB (City, Country)
     * @param locationName - The name of the location
     * @return The type of the location if found. If not found(or ambiguous), return null.
     */
    public static String getLocationType(String locationName) {
        ArrayList<Pair<String, String>> locationTypesToProperty = new ArrayList<Pair<String, String>>();
        locationTypesToProperty.add(new Pair("COUNTRY","dbo:Country"));
        locationTypesToProperty.add(new Pair("CITY","dbo:City"));
        locationTypesToProperty.add(new Pair("DISTRICT","yago:District108552138"));

        List<Map<String, String>> results = null;
        for(Pair<String, String> typeAndProperty : locationTypesToProperty) {
            results = SparqlExecuter.runSparql("select distinct ?location WHERE {\n" +
                    "        ?location a " + typeAndProperty.getValue() + " .\n" +
                    "        ?location foaf:name ?name .\n" +
                    "        FILTER (contains(lcase(str(?name)),\"" + locationName.toLowerCase() + "\")) .\n" +
                    "}", SparqlSources.DBPEDIA);
            if (results.size() >= 1)
                return typeAndProperty.getKey();
        }

        //Special check for states
        results = SparqlExecuter.runSparql("select distinct ?state WHERE {\n" +
                "?state a dbo:AdministrativeRegion .\n" +
                "?state dbo:country <http://dbpedia.org/resource/United_States> .\n" +
                "?district dbp:name \"" + locationName + "\"@en .\n" +
                "    ?state dbo:capital ?capital .\n" +
                "}", SparqlSources.DBPEDIA);
        if (results.size() >= 1)
            return "STATE";

        return null;
    }

    /***
     * Gets an artist's standard DBPedia name
     * @param rawName - The name to get
     * @return The artist's standard name if found. Null otherwise.
     */
    public static String getArtistStandardName(String rawName) {
        String query = "SELECT distinct (str(?name) as ?fixedName) WHERE {\n" +
                "      ?person a dbo:Artist .\n" +
                "      ?person foaf:name ?name .\n" +
                "      ?redirect dbo:wikiPageRedirects ?person .\n" +
                "      ?redirect rdfs:label ?label .\n" +
                "      FILTER (lcase(str(?label)) = \"" + rawName.toLowerCase().replace("_","") + "\")\n" +
                "}";
        List<Map<String, String>> results = SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA);
        if(results.size() != 1)
            return null;
        return results.get(0).get("fixedName");
    }

    /***
     * Gets a person's standard DBPedia name
     * @param rawName - The name to get
     * @param deepSearch - True if requires a thorough yet very slow search
     * @return The person's standard name if found. Null otherwise.
     */
    public static String getPersonStandardName(String rawName, boolean deepSearch) {
        try {
            String query = "SELECT distinct (str(?name) as ?fixedName) WHERE {\n" +
                    "      ?person a dbo:Person .\n" +
                    "      ?person foaf:name ?name .\n" +
                    "      FILTER (?name = \"" + rawName + "\"@en) .\n" +
                    "}";
            List<Map<String, String>> results = SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA);
            if (results.size() == 1)
                return results.get(0).get("fixedName") == null ? results.get(0).get("fixedLabel") : results.get(0).get("fixedName");

            query = "SELECT distinct (str(?name) as ?fixedName) WHERE {\n" +
                    "      ?person a dbo:Person .\n" +
                    "      ?person rdfs:label ?name .\n" +
                    "      FILTER (?name = \"" + rawName + "\"@en) .\n" +
                    "}";
            results = SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA);

            if(deepSearch) {
                if (results.size() == 1)
                    return results.get(0).get("fixedName");

                query = "SELECT distinct (str(?name) as ?fixedName) (str(?label) as ?fixedLabel) WHERE {\n" +
                        "      ?person a dbo:Person .\n" +
                        "      ?person foaf:name ?name .\n" +
                        "      FILTER ((lcase(str(?label)) = \"" + rawName.toLowerCase() + "\"))\n" +
                        "}";
                results = SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA);
                if (results.size() == 1)
                    return results.get(0).get("fixedName");


                query = "SELECT distinct (str(?name) as ?fixedName) (str(?label) as ?fixedLabel) WHERE {\n" +
                        "      ?person a dbo:Person .\n" +
                        "      ?person rdfs:label ?name .\n" +
                        "      FILTER ((lcase(str(?label)) = \"" + rawName.toLowerCase() + "\"))\n" +
                        "}";
                results = SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA);
                if (results.size() == 1)
                    return results.get(0).get("fixedName");

                query = "SELECT distinct (str(?name) as ?fixedName) WHERE {\n" +
                        "      ?person a dbo:Person .\n" +
                        "      ?person foaf:name ?name .\n" +
                        "      ?redirect dbo:wikiPageRedirects ?person .\n" +
                        "      ?redirect rdfs:label ?label .\n" +
                        "      FILTER (lcase(str(?label)) = \"" + rawName.toLowerCase().replace("_", "") + "\")\n" +
                        "}";
                results = SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA);
            }
            if (results.size() != 1)
                return null;
            return results.get(0).get("fixedName");
        } catch (RuntimeException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /***
     * Gets an artist's infomation from DBPedia
     * @param artistName - The artist's name
     * @return The artist info. Null if none found.
     */
    public static DBPediaArtist getArtistByName(String artistName) {
        String standardName = getArtistStandardName(artistName);
        if(standardName == null)
            return null;
        String query =
                "SELECT ?id ?name ?description (group_concat(distinct ?aField ; separator = \";\") AS ?field) (group_concat(distinct ?aMovement ; separator = \";\") AS ?movement) ?birthDate ?deathDate (group_concat(distinct ?aBirthPlace ; separator = \";\") AS ?birthPlace)  (group_concat(distinct ?aDeathPlace ; separator = \";\") AS ?deathPlace) (COUNT(DISTINCT ?links) AS ?totalLinks) (COUNT(DISTINCT ?art) AS ?totalArt) WHERE {\n" +
                        "      ?person a dbo:Artist .\n" +
                        "      ?person rdfs:comment ?description .\n" +
                        "      ?person dbo:wikiPageID ?id .\n" +
                        "      OPTIONAL {?person dbo:field ?aField} .\n" +
                        "      OPTIONAL {?person dbo:movement ?aMovement} .\n" +
                        "      ?person dbo:birthPlace ?aBirthPlace .\n" +
                        "      ?person dbo:birthDate ?birthDate .\n" +
                        "      OPTIONAL {?person dbo:deathPlace ?aDeathPlace} .\n" +
                        "      OPTIONAL {?person dbo:deathDate ?deathDate} .\n" +
                        "      ?person foaf:name ?name .\n" +
                        "      ?person dbo:wikiPageExternalLink ?links.\n" +
                        "      ?art    dbpedia2:artist ?person .\n" +
                        "      FILTER (LANG(?description) = 'en') . \n" +
                        "      FILTER NOT EXISTS {\n" +
                        "         ?person rdf:type <http://dbpedia.org/class/yago/Musician110339966> .\n" +
                        "      }\n" +
                        "      FILTER NOT EXISTS {\n" +
                        "         ?person rdf:type <http://dbpedia.org/ontology/MusicalArtist>\n" +
                        "      }\n" +
                        "      FILTER (str(?name) = \"" + standardName + "\")\n" +
                        "}\n" +
                        "GROUP BY ?id ?name ?description ?birthDate ?deathDate";

        List<Map<String, String>> results = SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA);
        if(results.size() != 1)
            return null;

        Map<String,String> artistProperties = results.get(0);
        return new DBPediaArtist(
                artistProperties.get("id"),
                artistProperties.get("name"),
                artistProperties.get("description"),
                artistProperties.get("field").split(";"),
                artistProperties.get("movement").split(";"),
                artistProperties.get("birthDate"),
                artistProperties.get("deathDate"),
                artistProperties.get("birthPlace").split(";"),
                artistProperties.get("deathPlace").split(";"),
                Integer.parseInt(artistProperties.get("totalLinks")),
                Integer.parseInt(artistProperties.get("totalArt"))
        );
    }

    public static DBPediaPerson getPersonByName(String personName) {
        String standardName = getPersonStandardName(personName, false);
        if(standardName == null)
            return null;
        String query =
                "SELECT ?id ?name ?description (group_concat(distinct ?aField ; separator = \";\") AS ?field) (group_concat(distinct ?aMovement ; separator = \";\") AS ?movement) ?birthDate ?deathDate (group_concat(distinct ?aBirthPlace ; separator = \";\") AS ?birthPlace)  (group_concat(distinct ?aDeathPlace ; separator = \";\") AS ?deathPlace) (COUNT(DISTINCT ?links) AS ?totalLinks) WHERE {\n" +
                        "      ?person a dbo:Person .\n" +
                        "      ?person rdfs:comment ?description .\n" +
                        "      ?person dbo:wikiPageID ?id .\n" +
                        "      OPTIONAL {?person dbo:field ?aField} .\n" +
                        "      OPTIONAL {?person dbo:movement ?aMovement} .\n" +
                        "      OPTIONAL {?person dbo:birthPlace ?aBirthPlace} .\n" +
                        "      OPTIONAL {?person dbo:birthDate ?birthDate} .\n" +
                        "      OPTIONAL {?person dbo:deathPlace ?aDeathPlace} .\n" +
                        "      OPTIONAL {?person dbo:deathDate ?deathDate} .\n" +
                        "      ?person  foaf:name ?name .\n" +
                        "      OPTIONAL {?person rdfs:label ?label} .\n" +
                        "      OPTIONAL {?person dbo:wikiPageExternalLink ?links} .\n" +
                        "      FILTER (LANG(?description) = 'en') .\n" +
                        "      FILTER (?name = \"" + personName + "\"@en) .\n" +
                        "}\n" +
                        "GROUP BY ?id ?name ?description ?birthDate ?deathDate";

        List<Map<String, String>> results = SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA);
        if(results.size() != 1) {
            query = "SELECT ?id ?name ?description (group_concat(distinct ?aField ; separator = \";\") AS ?field) (group_concat(distinct ?aMovement ; separator = \";\") AS ?movement) ?birthDate ?deathDate (group_concat(distinct ?aBirthPlace ; separator = \";\") AS ?birthPlace)  (group_concat(distinct ?aDeathPlace ; separator = \";\") AS ?deathPlace) (COUNT(DISTINCT ?links) AS ?totalLinks) WHERE {\n" +
                    "      ?person a dbo:Person .\n" +
                    "      ?person rdfs:comment ?description .\n" +
                    "      ?person dbo:wikiPageID ?id .\n" +
                    "      OPTIONAL {?person dbo:field ?aField} .\n" +
                    "      OPTIONAL {?person dbo:movement ?aMovement} .\n" +
                    "      OPTIONAL {?person dbo:birthPlace ?aBirthPlace} .\n" +
                    "      OPTIONAL {?person dbo:birthDate ?birthDate} .\n" +
                    "      OPTIONAL {?person dbo:deathPlace ?aDeathPlace} .\n" +
                    "      OPTIONAL {?person dbo:deathDate ?deathDate} .\n" +
                    "      ?person  rdfs:label ?name .\n" +
                    "      OPTIONAL {?person rdfs:label ?label} .\n" +
                    "      OPTIONAL {?person dbo:wikiPageExternalLink ?links} .\n" +
                    "      FILTER (LANG(?description) = 'en') .\n" +
                    "      FILTER (?name = \"" + personName + "\"@en) .\n" +
                    "}\n" +
                    "GROUP BY ?id ?name ?description ?birthDate ?deathDate";

            results = SparqlExecuter.runSparql(query,SparqlSources.DBPEDIA);
            if(results.size() != 1)
                return null;
        }


        Map<String,String> artistProperties = results.get(0);
        return new DBPediaPerson(
                artistProperties.get("id"),
                artistProperties.get("name"),
                artistProperties.get("description"),
                artistProperties.get("birthDate"),
                artistProperties.get("deathDate"),
                artistProperties.get("birthPlace").split(";"),
                artistProperties.get("deathPlace").split(";"),
                Integer.parseInt(artistProperties.get("totalLinks")));
    }
}
