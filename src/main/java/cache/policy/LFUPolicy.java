package cache.policy;

import cache.CacheItem;

import java.util.List;
import java.util.stream.IntStream;

public class LFUPolicy<T> implements ReplacementPolicy<T> {
    @Override
    public boolean clean(List<CacheItem<T>> cacheItems, int maxSize, float proportion) {
        int newSize = (int) (maxSize * proportion);
        while (cacheItems.size() > newSize) {
            int minIdx = IntStream.range(0, cacheItems.size())
                    .reduce((i, j) -> cacheItems.get(i).getNumAccesses() > cacheItems.get(j).getNumAccesses() ? j : i)
                    .getAsInt();

            System.out.println("Removing cache index " + minIdx);
            cacheItems.remove(0);
        }

        return true;
    }
}
