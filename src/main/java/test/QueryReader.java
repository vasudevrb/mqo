package test;

import common.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class QueryReader {

    private static final String filePath = "resources/der/70/query_templates.txt";
    private static final String queryMetadataPath = "resources/der/70/query_template_md.txt";
    private static final String batchMetadataPath = "resources/batchables.txt";

    private static List<String> readQueries() throws IOException {
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

    private static List<List<Integer>> readBatchMetadata(int scale) throws IOException {
        List<List<Integer>> batchMd = new ArrayList<>();

        FileUtils.readLines(new File(batchMetadataPath), Charset.defaultCharset())
                .stream()
                .filter(line -> !line.startsWith("%") && !line.isEmpty())
                .forEach(line -> {
                    List<Integer> ints = new ArrayList<>();
                    String[] seps = StringUtils.splitByWholeSeparator(line, ",");
                    for (String sep : seps) {
                        int x = Integer.parseInt(sep.trim());
                        ints.add(x);
                        while (x + 32 < (32 * scale)) {
                            x += 32;
                            ints.add(x);
                        }
                    }
                    batchMd.add(ints);
                });

        return batchMd;
    }

    // Scale is in the order of 32. So a scale of 10 would mean 320 queries
    public static List<List<String>> getQueries(int scale) throws IOException {
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

        return batch(queries, scale);
    }

    private static List<List<String>> batch(List<String> queries, int scale) throws IOException {
        List<List<Integer>> batchMd = readBatchMetadata(scale);

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

    public static List<List<String>> getQueries() throws IOException {
        return getQueries(10);
    }

    public static void main(String[] args) throws IOException {
        List<List<String>> q = getQueries(20);
        System.out.println("A");
    }
}
