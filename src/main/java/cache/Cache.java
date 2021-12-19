package cache;

import cache.dim.Dimension;
import cache.policy.ReplacementPolicy;

import java.util.ArrayList;
import java.util.List;

import static common.Logger.logCache;
import static common.Logger.logTime;
import static common.Utils.humanReadable;

public class Cache<T> {

    public static final List<Integer> SIZES_MB = List.of(4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);

    private final List<CacheItem<T>> items;
    private final Dimension dimension;
    private final ReplacementPolicy<T> policy;

    private final CacheSizeWatcher cacheSizeWatcher;

    // This variable depends on the dimension that the cache is evaluated on.
    // If dimension is SIZE, then this value is the max allowed size in bytes.
    // If dimension is COUNT, then this value is the size of the list items.
    private long currentCacheSize;

    public Cache(ReplacementPolicy<T> policy, Dimension dimension) {
        this.items = new ArrayList<>();
        this.policy = policy;
        this.dimension = dimension;

        this.cacheSizeWatcher = () -> {
            if (currentCacheSize > this.dimension.getValue() * ReplacementPolicy.PRUNE_THRESHOLD) {
                logCache("Cleaning cache!!");
                clean();
            }
        };
    }

    public synchronized void add(T item, long value) {
        this.items.add(new CacheItem<>(item, value));
        currentCacheSize += value;
        logTime("Added item: Cache size is " + formatCacheSize());
        cacheSizeWatcher.onCacheSizeChange();
    }

    public List<CacheItem<T>> getItems() {
        return items;
    }

    public void clean() {
        currentCacheSize = policy.clean(items, currentCacheSize, dimension);
        logCache("After clean: current size is " + formatCacheSize() + ", length: " + items.size());
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
