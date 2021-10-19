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

    private String buildString() {
        List<String> strs = new ArrayList<>();
        for (Term term : terms) {
            if (term instanceof Operator) {
                strs.add(((Operator) term).buildString());
            } else if (term instanceof Predicate) {
                strs.add(term.toString());
            }
        }
        return "(" + String.join(" " + type.name() + " ", strs) + ")";
    }

    @Override
    public String toString() {
//        return type + "(" + Arrays.toString(terms.toArray()) + ")";
        return buildString();
    }

    public enum Type {
        AND, OR
    }
}
