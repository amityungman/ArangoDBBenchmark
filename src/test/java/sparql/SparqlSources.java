package sparql;

/**
 * Created by Amit on 18/10/2016.
 */
public enum SparqlSources {
    DBPEDIA;

    public String getEndpoint() {
        switch(this) {
            case DBPEDIA:
                return "http://dbpedia.org/sparql";
        }
        throw new RuntimeException("An internal Error Occurred in the SparqlSources enum");
    }

    public String getPrefices() {
        switch(this) {
            case DBPEDIA:
                return "PREFIX dbo: <http://dbpedia.org/ontology/>\n" +
                        "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
                        "PREFIX dct: <http://purl.org/dc/terms/>\n" +
                        "PREFIX dc: <http://purl.org/dc/elements/1.1/>\n" +
                        "PREFIX : <http://dbpedia.org/resource/>\n" +
                        "PREFIX dbpedia2: <http://dbpedia.org/property/>\n" +
                        "PREFIX dbpedia: <http://dbpedia.org/>\n" +
                        "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n";
        }
        throw new RuntimeException("An internal Error Occurred in the SparqlSources enum");
    }
}
