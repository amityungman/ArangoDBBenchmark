package sparql;



import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Execute SPARQL queries
 * Created by Amit on 18/10/2016.
 */
public class SparqlExecuter {

    /***
     * Run a SPARQL query for a known source.
     * @param sparqlQuery - The query to run
     * @param sparqlSource - The source to run for
     * @return A list of all the retrieved results. Each results has a map for field name and value
     */
    public static List<Map<String,String>> runSparql(String sparqlQuery, SparqlSources sparqlSource) {
        return runSparql(sparqlSource.getPrefices() + sparqlQuery, sparqlSource.getEndpoint());
    }

    /***
     * Run a SPARQL query
     * @param sparqlQuery - The query to run
     * @param sparqlEndpoint - The end point of the source
     * @return A list of all the retrieved results. Each results has a map for field name and value
     */
    public static List<Map<String,String>> runSparql(String sparqlQuery, String sparqlEndpoint) {
        //Create query and connection
        Query query = QueryFactory.create(sparqlQuery, Syntax.syntaxSPARQL) ;
        QuerySolutionMap querySolutionMap = new QuerySolutionMap();
        ParameterizedSparqlString parameterizedSparqlString = new ParameterizedSparqlString(query.toString(), querySolutionMap);
        QueryEngineHTTP httpQuery = new QueryEngineHTTP(sparqlEndpoint,parameterizedSparqlString.asQuery());
        //Run Query
        ResultSet results = httpQuery.execSelect();
        //Save results to list-map
        List<Map<String,String>> resultsSet = new LinkedList<Map<String,String>>();
        while (results.hasNext()) {
            Map<String,String> resultsMap = new HashMap<String,String>();
            QuerySolution solution = results.next();
            for(String varName : results.getResultVars()) {
                String expressionValue = "";
                if(solution.contains(varName))
                    expressionValue = solution.get(varName).isResource() ? solution.get(varName).asResource().getURI() : solution.get(varName).asLiteral().getLexicalForm();
                //if(solution.get(varName).isResource())
                expressionValue = getURITerminals(expressionValue, ";").replace("_"," ");
                resultsMap.put(varName,expressionValue);
            }
            resultsSet.add(resultsMap);
        }

        return resultsSet;
    }

    /***
     * Returns the terminal from a URI
     * @param originalURI - The uri to parse
     * @param separator - The seperator for multiple URIs
     * @return The list of terminals extracted from the URIs, separated by the separator
     */
    private static String getURITerminals(String originalURI, String separator) {
        StringBuilder sb = new StringBuilder();
        for(String uri : originalURI.split(separator))
            sb.append(uri.substring(uri.lastIndexOf("/")+1)).append(separator);

        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
}
