package test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class QueryReader {

    private static final String filePath = "resources/queries.txt";

    public static List<List<String>> getQueries() throws IOException {
        List<List<String>> queries = new ArrayList<>();

        List<String> lines = FileUtils.readLines(new File(filePath));
        String groupSep = lines.get(0);
        String querySep = lines.get(1);

        String data = lines
                .subList(2, lines.size())
                .stream()
                .filter(line -> !line.startsWith("%"))
                .collect(Collectors.joining(" "));

        String[] queryGroups = StringUtils.splitByWholeSeparator(data, groupSep);
        for (String group : queryGroups) {
            List<String> qs = Arrays.stream(StringUtils.splitByWholeSeparator(group, querySep))
                    .map(String::trim)
                    .collect(Collectors.toList());

            queries.add(qs);
        }

        return queries;
    }

    public static void main(String[] args) {
        try {
            System.out.println(QueryReader.getQueries().get(0));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
