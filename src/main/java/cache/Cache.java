package cache;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Cache<T> {

    private final List<CacheItem<T>> items;
    private int size;

    public Cache(int size) {
        this.items = new ArrayList<>();
        this.size = size;
    }

    public void addItem(T item) {
        this.items.add(new CacheItem<>(item));
    }

    public List<T> getItems() {
        return items.stream().map(CacheItem::getItem).collect(Collectors.toList());
    }

    public void incrementNumAccesses(T item) {
        items.stream().filter(i -> i.getItem() == item).forEach(CacheItem::incrementNumAccesses);
    }

    public boolean clean() {
        return true;
    }
}
