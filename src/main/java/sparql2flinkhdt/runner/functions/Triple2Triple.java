package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

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
        Integer s = null, p = null, o = null;

        // Mensajes para indicar qué triple se está verificando
        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
                t.getSubject(), t.getPredicate(), t.getObject()));

        // Comparar los IDs según los valores proporcionados
        if (subject == null && predicate != null && object != null) {
            // Filtrado solo por predicado y objeto
            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);

            if (t.getPredicate() == p.longValue() && t.getObject() == o.longValue()) {
                System.out.println("Predicado y objeto coinciden");
                return true;
            } else {
                System.out.println(String.format("Predicado o objeto no coinciden: esperado predicado=%d, objeto=%d; encontrado predicado=%d, objeto=%d", p, o, t.getPredicate(), t.getObject()));
                return false;
            }

        } else if (subject != null && predicate == null && object != null) {
            // Filtrado por sujeto y objeto
            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);

            if (t.getSubject() == s.longValue() && t.getObject() == o.longValue()) {
                System.out.println("Sujeto y objeto coinciden");
                return true;
            } else {
                System.out.println(String.format("Sujeto o objeto no coinciden: esperado sujeto=%d, objeto=%d; encontrado sujeto=%d, objeto=%d", s, o, t.getSubject(), t.getObject()));
                return false;
            }

        } else if (subject != null && predicate != null && object == null) {
            // Filtrado por sujeto y predicado
            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);

            if (t.getSubject() == s.longValue() && t.getPredicate() == p.longValue()) {
                System.out.println("Sujeto y predicado coinciden");
                return true;
            } else {
                System.out.println(String.format("Sujeto o predicado no coinciden: esperado sujeto=%d, predicado=%d; encontrado sujeto=%d, predicado=%d", s, p, t.getSubject(), t.getPredicate()));
                return false;
            }

        } else if (subject != null && predicate == null && object == null) {
            // Filtrado solo por sujeto
            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);

            if (t.getSubject() == s.longValue()) {
                System.out.println("Sujeto coincide");
                return true;
            } else {
                System.out.println(String.format("Sujeto no coincide: esperado sujeto=%d, encontrado sujeto=%d", s, t.getSubject()));
                return false;
            }

        } else if (subject == null && predicate != null && object == null) {
            // Filtrado solo por predicado
            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);

            if (t.getPredicate() == p.longValue()) {
                System.out.println("Predicado coincide");
                return true;
            } else {
                System.out.println(String.format("Predicado no coincide: esperado predicado=%d, encontrado predicado=%d", p, t.getPredicate()));
                return false;
            }

        } else if (subject == null && predicate == null && object != null) {
            // Filtrado solo por objeto
            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);

            if (t.getObject() == o.longValue()) {
                System.out.println("Objeto coincide");
                return true;
            } else {
                System.out.println(String.format("Objeto no coincide: esperado objeto=%d, encontrado objeto=%d", o, t.getObject()));
                return false;
            }

        } else {
            // Si no hay filtros definidos, acepta todos los triples
            System.out.println("No hay filtros, triple aceptado");
            return true;
        }
    }
}
