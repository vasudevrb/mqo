package cache.policy;

import cache.CacheItem;

import java.util.List;

public class FIFOPolicy<T> implements ReplacementPolicy<T> {
    @Override
    public boolean clean(List<CacheItem<T>> cacheItems, int maxSize, float proportion) {
        int newSize = (int) (maxSize * proportion);
        while (cacheItems.size() > newSize) {
            cacheItems.remove(0);
        }
        return true;
    }
}
