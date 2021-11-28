package cache;

public class CacheItem<T> {
    private T item;

    private long lastAccessTime;
    private int numAccesses;

    public CacheItem(T item) {
        this.item = item;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public T getItem() {
        lastAccessTime = System.currentTimeMillis();
        numAccesses++;
        return item;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public void incrementNumAccesses() {
        numAccesses++;
    }

    public int getNumAccesses() {
        return numAccesses;
    }
}
