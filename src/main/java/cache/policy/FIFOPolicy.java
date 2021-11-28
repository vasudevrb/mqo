package cache.policy;

import cache.CacheItem;
import cache.dim.Dimension;

import java.util.List;

public class FIFOPolicy<T> implements ReplacementPolicy<T> {

    @Override
    public long clean(List<CacheItem<T>> cacheItems, long currentSize, Dimension dimension, float proportion) {
        int newSize = (int) (dimension.getValue() * proportion);
        while (currentSize > newSize) {
            currentSize -= cacheItems.get(0).getValue();
            System.out.println("Removing cache index " + 0);
            cacheItems.remove(0);
        }

        return currentSize;
    }
}
