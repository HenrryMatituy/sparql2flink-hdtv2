package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

// Triple to Triple - Filter Function
public class Triple2Triple implements FilterFunction<TripleID> {

    private SerializableDictionary dictionary;
    private String subject, predicate, object;

    public Triple2Triple(SerializableDictionary dictionary, String subject, String predicate, String object) {
        this.dictionary = dictionary;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    @Override
    public boolean filter(TripleID t) {
        Integer s, p, o;
        if (subject == null && predicate != null && object != null) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return (t.getPredicate() == p && t.getObject() == o);
        } else if (subject != null && predicate == null && object != null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return (t.getSubject() == s && t.getObject() == o);
        } else if (subject != null && predicate != null && object == null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            return (t.getSubject() == s && t.getPredicate() == p);
        } else if (subject != null && predicate == null && object == null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            return t.getSubject() == s;
        } else if (subject == null && predicate != null && object == null) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            return t.getPredicate() == p;
        } else if (subject == null && predicate == null && object != null) {
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            return t.getObject() == o;
        } else {
            return true;
        }
    }
}
