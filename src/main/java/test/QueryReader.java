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

    private static final String filePath = "resources/query_templates.txt";
    private static final String metadataPath = "resources/query_template_md.txt";

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

    private static List<List<List<Double>>> readQueryMetadata() throws IOException {
        List<List<List<Double>>> metadata = new ArrayList<>();

        List<String> lines = FileUtils.readLines(new File(metadataPath), Charset.defaultCharset());

        lines.stream()
                .forEach(line -> {
                    List<List<Double>> lineList = new ArrayList<>();
                    String[] minMaxSet = StringUtils.substringsBetween(line, "[", "]");
                    for (String mnMx : minMaxSet) {
                        String[] seps = StringUtils.splitByWholeSeparator(mnMx, " ");
                        double mn = Double.parseDouble(seps[0]);
                        double mx = Double.parseDouble(seps[1]);
                        lineList.add(List.of(mn, mx));
                    }
                    metadata.add(lineList);
                });

        return metadata;
    }

    // Scale is in the order of 32. So a scale of 10 would mean 320 queries
    public static List<String> getQueries(int scale) throws IOException {
        List<String> queryTemplates = readQueries();
        List<List<List<Double>>> md = readQueryMetadata();

        List<String> queries = new ArrayList<>();

        for (int k = 0; k < scale; k++) {
            for (int i = 0; i < queryTemplates.size(); i++) {
                String template = queryTemplates.get(i);
                if (!template.contains("%")) {
                    queries.add(template);
                    continue;
                }

                Double[] values = md.get(i).stream().map(Utils::getQueryOperandBetween).toArray(Double[]::new);
                queries.add(String.format(template, (Object[]) values));
            }
        }

        return queries;
    }

    public static List<String> getQueries() throws IOException {
        return getQueries(10);
    }
}
