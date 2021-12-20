package cache;

import cache.dim.Dimension;
import cache.policy.ReplacementPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static common.Logger.logCache;
import static common.Logger.logTime;
import static common.Utils.humanReadable;

public class Cache<T> {

    public static final List<Integer> SIZES_MB = List.of(4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);

    private final HashMap<String, List<CacheItem<T>>> map;

    private final Dimension dimension;
    private final ReplacementPolicy<T> policy;

    private final CacheSizeWatcher cacheSizeWatcher;

    // This variable depends on the dimension that the cache is evaluated on.
    // If dimension is SIZE, then this value is the max allowed size in bytes.
    // If dimension is COUNT, then this value is the size of the list items.
    private long currentCacheSize;

    public Cache(ReplacementPolicy<T> policy, Dimension dimension) {
        this.map = new HashMap<>();
        this.policy = policy;
        this.dimension = dimension;

        this.cacheSizeWatcher = () -> {
            if (currentCacheSize > this.dimension.getValue() * ReplacementPolicy.PRUNE_THRESHOLD) {
                logCache("Cleaning cache!!");
                clean();
            }
        };
    }

    public void add(T item, String key, long value) {
        if (map.containsKey(key)) {
            map.get(key).add(new CacheItem<>(item, key, value));
        } else {
            var indexes = new ArrayList<CacheItem<T>>();
            indexes.add(new CacheItem<>(item, key, value));
            map.put(key, indexes);
        }

        currentCacheSize += value;
        logTime("Added item: Cache size is " + formatCacheSize());
        cacheSizeWatcher.onCacheSizeChange();
    }

    public List<T> find(String key) {
        if (!map.containsKey(key)) {
            return new ArrayList<>();
        }

        return map.get(key).stream().map(CacheItem::getItem).collect(Collectors.toList());
    }

    public void clean() {
        Map<String, List<CacheItem<T>>> removables = policy.getRemovableIndexes(map, currentCacheSize, dimension);

        for (Map.Entry<String, List<CacheItem<T>>> entry : removables.entrySet()) {
            map.get(entry.getKey()).removeAll(entry.getValue());
            entry.getValue().forEach(x -> this.currentCacheSize -= x.getValue());
        }

        logCache("Cache cleaned. Size: " + formatCacheSize());
    }

    private String formatCacheSize() {
        return dimension.getType() == Dimension.Type.COUNT
                ? String.valueOf(currentCacheSize) :
                humanReadable(currentCacheSize);
    }

    public Dimension getDimension() {
        return dimension;
    }

    public interface CacheSizeWatcher {
        void onCacheSizeChange();
    }
}
