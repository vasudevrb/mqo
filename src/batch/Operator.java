package batch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public List<String> getAllPredicateNames(Function<String, Boolean> exclusion) {
        List<String> pNames = terms.stream()
                .filter(t -> t instanceof Predicate)
                .filter(pr -> exclusion.apply(((Predicate) pr).getValue())) //TODO Maybe just check predicate.isJoin instead of string ops
                .map(t -> ((Predicate) t).getName().replaceAll("`", "\""))
                .collect(Collectors.toList());
        terms.stream()
                .filter(t -> t instanceof Operator)
                .forEach(t -> pNames.addAll(((Operator) t).getAllPredicateNames(exclusion)));
        return pNames;
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
        return buildString();
    }

    public enum Type {
        AND, OR
    }
}
