package sparql;

import sparql.dbpediaObjects.DBPediaArtist;
import sparql.dbpediaObjects.DBPediaArtwork;

import java.util.*;

/**
 * Queries over DBPedia with sparql.
 * Created by Amit on 18/10/2016.
 */
public class DBPediaSparqlQuerier {

    /***
     * Gets the most famous artists in the DBPedia database.
     * @param maxAmount - The maximal amount of artists wanted
     * @return A list of the most famous artists and properties about them -
     * name, description, field[] (in art), movement[] (in art), birthDate, deathDate, birthPlace[], deathPlace[], totalLinks (external links in WIKI), totalArt (artwork listed in WIKI)
     */
    public static List<DBPediaArtist> getMostFamousArtists(int maxAmount) {
        List<DBPediaArtist> artists = new LinkedList<DBPediaArtist>();
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

        for(Map<String,String> artistProperties : SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA)) {
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
        for(Map<String,String> result : SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA))
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

        for(Map<String,String> result : SparqlExecuter.runSparql(query, SparqlSources.DBPEDIA))
            artwork.add(new DBPediaArtwork(result.get("id"), result.get("art"), result.get("description"), getCreationYearFromSubjects(result.get("subject").split(";"))));

        return artwork;
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
}
