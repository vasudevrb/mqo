package test;

import common.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class QueryReader {

    public static final int TYPE_ALL = 0;
    public static final int TYPE_SIMPLE_FILTER = 1;
    public static final int TYPE_COMPLEX_FILTER = 2;
    public static final int TYPE_FILTER_JOIN = 3;
    public static final int TYPE_FILTER_AGGREGATE = 4;
    public static final int TYPE_FILTER_JOIN_AGGREGATE = 5;

    public static String dir;

    private static final String batchMetadataPath = "resources/batchables.txt";

    private static List<String> readQueries() throws IOException {
        String filePath = "resources/der/" + dir + "/query_templates.txt";
        System.out.println("Reading queries : " + filePath);

        List<String> queries = new ArrayList<>();

        List<String> lines = FileUtils.readLines(new File(filePath), Charset.defaultCharset());
        String groupSep = lines.get(0);
        String querySep = lines.get(1);

        String data = lines
                .subList(2, lines.size())
                .stream()
                .filter(line -> !line.startsWith("%"))
                .collect(Collectors.joining(" "));

        String[] queryGroups = StringUtils.splitByWholeSeparator(data, groupSep);
        for (String group : queryGroups) {
            Arrays.stream(StringUtils.splitByWholeSeparator(group, querySep))
                    .map(String::trim)
                    .forEach(queries::add);
        }

        return queries;
    }

    private static List<List<List<String>>> readQueryMetadata() throws IOException {
        String queryMetadataPath = "resources/der/" + dir + "/query_template_md.txt";
        System.out.println("Reading metadata : " + queryMetadataPath);


        List<List<List<String>>> metadata = new ArrayList<>();

        List<String> lines = FileUtils.readLines(new File(queryMetadataPath), Charset.defaultCharset());

        lines.stream()
                .forEach(line -> {
                    List<List<String>> lineList = new ArrayList<>();
                    String[] minMaxSet = StringUtils.substringsBetween(line, "[", "]");
                    for (String mnMx : minMaxSet) {
                        String[] seps = StringUtils.splitByWholeSeparator(mnMx, " ");
                        lineList.add(List.of(seps[0], seps[1]));

                    }
                    metadata.add(lineList);
                });

        return metadata;
    }

    private static List<List<Integer>> readBatchMetadata(List<String> queries) {
        String exc1 = "FROM \"customer\" WHERE \"c_acctbal\" between ";
        String exc2_1 = "FROM \"lineitem\" WHERE \"l_tax\" ";
        String exc2_2 = "FROM \"lineitem\" WHERE \"l_quantity\" ";

        Map<String, List<Integer>> map = new HashMap<>();
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            String from;

            if (query.contains(exc1)) {
                from = exc1;
            } else if (query.contains(exc2_1) || query.contains(exc2_2)) {
                from = exc2_1;
            } else {
                from = StringUtils.substringBetween(queries.get(i), "FROM ", "WHERE ").trim();
            }

            if (map.containsKey(from)) {
                map.get(from).add(i);
            } else {
                ArrayList<Integer> indexes = new ArrayList<>();
                indexes.add(i);
                map.put(from, indexes);
            }
        }

        return new ArrayList<>(map.values());
    }

    // Scale is in the order of 32. So a scale of 10 would mean 320 queries
    public static List<List<String>> getQueries(int scale, int type) throws IOException {
        List<String> queryTemplates = readQueries();
        List<List<List<String>>> md = readQueryMetadata();

        List<String> queries = new ArrayList<>();

        for (int k = 0; k < scale; k++) {
            for (int i = 0; i < queryTemplates.size(); i++) {
                String template = queryTemplates.get(i);
                if (!template.contains("%")) {
                    queries.add(template);
                    continue;
                }

                List<Integer> argInds = Utils.getAllTemplateArgInds(template);
                List<Object> vals = new ArrayList<>();

                for (int z = 0; z < argInds.size(); z++) {
                    int argIndex = argInds.get(z);
                    List<String> v = md.get(i).get(z);
                    if (template.startsWith("%d", argIndex)) {
                        int mn = Integer.parseInt(v.get(0));
                        int mx = Integer.parseInt(v.get(1));
                        vals.add(Utils.getQueryOperandBetween(mn, mx));
                    } else {
                        double mn = Double.parseDouble(v.get(0));
                        double mx = Double.parseDouble(v.get(1));
                        vals.add(Utils.getQueryOperandBetween(mn, mx));
                    }
                }

                queries.add(String.format(template, vals.toArray()));
            }
        }

        return batch(reduceTypes(queries, type), scale);
    }

    private static List<List<String>> batch(List<String> queries, int scale) throws IOException {
        List<List<Integer>> batchMd = readBatchMetadata(queries);

        List<List<String>> batchedQueries = new ArrayList<>();
        List<Integer> alreadyBatched = new ArrayList<>();

        for (int i = 0; i < queries.size(); i++) {
            if (alreadyBatched.contains(i)) {
                continue;
            }

            String query = queries.get(i);

            if (Utils.shouldBatch()) {
                List<Integer> batchCandidateIndexes = Utils.getBatchCandidateIndexes(queries, i, batchMd);
                batchCandidateIndexes = batchCandidateIndexes.stream().filter(bc -> !alreadyBatched.contains(bc)).toList();

                alreadyBatched.add(i);
                alreadyBatched.addAll(batchCandidateIndexes);

                List<String> batchedList = new ArrayList<>();
                batchedList.add(query);
                batchCandidateIndexes.forEach(bc -> batchedList.add(queries.get(bc)));

                batchedQueries.add(batchedList);
            } else {
                batchedQueries.add(List.of(query));
                alreadyBatched.add(i);
            }
        }
        return batchedQueries;
    }

    private static List<String> reduceTypes(List<String> queries, int type) {
        var simpleFilterIndexes = List.of(24, 25, 26, 27, 28, 29, 30, 31);
        var complexFilterIndexes = List.of(0, 1, 2, 8, 9);
        var filterJoinIndexes = List.of(3, 4, 5, 6, 7);
        var filterAggregateIndexes = List.of(10, 11, 12, 13, 14, 15, 16);
        var filterJoinAggregateIndexes = List.of(17, 18, 19, 20, 21, 22, 23);
        
        if (type == TYPE_ALL) {
            return queries;
        }
        
        List<String> reducedQueries = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            if (type == TYPE_SIMPLE_FILTER && simpleFilterIndexes.contains(i % 32)) reducedQueries.add(queries.get(i));
            else if (type == TYPE_COMPLEX_FILTER && complexFilterIndexes.contains(i % 32)) reducedQueries.add(queries.get(i));
            else if (type == TYPE_FILTER_JOIN && filterJoinIndexes.contains(i % 32)) reducedQueries.add(queries.get(i));
            else if (type == TYPE_FILTER_AGGREGATE && filterAggregateIndexes.contains(i % 32)) reducedQueries.add(queries.get(i));
            else if (type == TYPE_FILTER_JOIN_AGGREGATE && filterJoinAggregateIndexes.contains(i % 32)) reducedQueries.add(queries.get(i));
        }

        return reducedQueries;
    }

    public static void main(String[] args) throws IOException {
        QueryReader.dir = "40";
        List<List<String>> simpleFilter = getQueries(10, TYPE_FILTER_JOIN_AGGREGATE);

        System.out.println("A");
    }
}
