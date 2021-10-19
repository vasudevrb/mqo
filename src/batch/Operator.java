package batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Operator extends Term {
    public List<Term> terms;
    public Type type;

    public Operator(Type type) {
        this.type = type;
        this.terms = new ArrayList<>();
    }

    public void addTerm(Term term) {
        this.terms.add(term);
    }

    public enum Type {
        AND, OR
    }

    @Override
    public String toString() {
        return "Operator{" +
                "type=" + type +
                ", terms=" + Arrays.toString(terms.toArray()) +
                '}';
    }
}
