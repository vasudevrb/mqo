package batch;

import org.apache.commons.jexl3.MapContext;

public class DefaultMapContext extends MapContext {

    @Override
    public boolean has(String name) {
        return true;
    }

    @Override
    public Object get(String name) {
        return super.get(name) != null ? super.get(name) : true;
    }
}
