package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;

import java.util.List;

public interface ReplacementPolicy<T> {

    float PRUNE_THRESHOLD = 0.8f;
    float PRUNE_TO = 0.7f;

    default long clean(List<CacheItem<T>> items, long currentSize, Dimension dimension) {
        return clean(items, currentSize, dimension, PRUNE_TO);
    }

    // Returns the size of the cache after cleaning
    long clean(List<CacheItem<T>> items, long currentSize, Dimension dimension, float proportion);
}
