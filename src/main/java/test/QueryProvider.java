package test;

import java.io.IOException;
import java.util.List;

public class QueryProvider {

    public List<List<String>> queries;

    public QueryProvider() {
        try {
            queries = QueryReader.getQueries(10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
