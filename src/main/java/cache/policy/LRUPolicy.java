package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;
import kotlin.Pair;

import java.util.*;
import java.util.stream.IntStream;

public class LRUPolicy<T> implements ReplacementPolicy<T> {

    @Override
    public List<Integer> getRemovableIndexes(List<CacheItem<T>> items, long currentSize, Dimension dimension) {
        int requiredSize = (int) (dimension.getValue() * PRUNE_TO);

        List<Integer> sortedByTime = IntStream.range(0, items.size())
                .boxed()
                .sorted(Comparator.comparingLong(i -> items.get(i).getLastAccessTime()))
                .toList();

        List<Integer> removables = new ArrayList<>();
        int i = 0;

        while (currentSize > requiredSize) {
            int index = sortedByTime.get(i);
            currentSize -= items.get(index).getValue();
            removables.add(index);
            i++;
        }

        removables.sort(Collections.reverseOrder());

        return removables;
    }

    @Override
    public Map<String, List<CacheItem<T>>> getRemovableIndexes(Map<String, List<CacheItem<T>>> map, long currentSize, Dimension dimension) {
        int requiredSize = (int) (dimension.getValue() * PRUNE_TO);
        Map<String, List<CacheItem<T>>> removableMap = new HashMap<>();

        List<Pair<String, CacheItem<T>>> allItems = map.keySet().stream()
                .map(x -> new Pair<>(x, map.get(x)))
                .flatMap(x -> x.getSecond().stream().map(k -> new Pair<>(x.getFirst(), k)))
                .toList();

        List<Integer> sortedByTime = IntStream.range(0, allItems.size())
                .boxed()
                .sorted(Comparator.comparingLong(i -> allItems.get(i).getSecond().getLastAccessTime()))
                .toList();

        int i = 0;

        while (currentSize > requiredSize && i < allItems.size()) {
            int index = sortedByTime.get(i);
            Pair<String, CacheItem<T>> p = allItems.get(index);
            currentSize -= p.getSecond().getValue();

            if (removableMap.containsKey(p.getFirst())) {
                removableMap.get(p.getFirst()).add(p.getSecond());
            } else {
                List<CacheItem<T>> it = new ArrayList<>();
                it.add(p.getSecond());
                removableMap.put(p.getFirst(), it);
            }

            i++;
        }

        return removableMap;
    }

    @Override
    public String toString() {
        return "LRU";
    }
}
