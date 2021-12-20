package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;

import java.util.List;
import java.util.Map;

public interface ReplacementPolicy<T> {

    float PRUNE_THRESHOLD = 0.8f;
    float PRUNE_TO = 0.7f;

    List<Integer> getRemovableIndexes(List<CacheItem<T>> items, long currentSize, Dimension dimension);

    Map<String, List<CacheItem<T>>> getRemovableIndexes(Map<String, List<CacheItem<T>>> items, long currentSize, Dimension dimension);
}
