package batch;

import java.util.ArrayList;
import java.util.List;

public class Expression {
    private List<Term> terms;

    public Expression() {
        this.terms = new ArrayList<>();
    }

    public Expression(List<Term> terms) {
        this.terms = terms;
    }

    public List<Term> getTerms() {
        return terms;
    }

    public void addTerm(Term term) {
        this.terms.add(term);
    }
}
