package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.enums.TripleComponentRole;
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
        Integer s = null, p = null, o = null;

        // Mapear IDs a URIs
        String uriSubject = dictionary.idToString((int) t.getSubject(), TripleComponentRole.SUBJECT);
        String uriPredicate = dictionary.idToString((int) t.getPredicate(), TripleComponentRole.PREDICATE);
        String uriObject = dictionary.idToString((int) t.getObject(), TripleComponentRole.OBJECT);

        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
                t.getSubject(), t.getPredicate(), t.getObject()));
        System.out.println(String.format("Triple actual: Sujeto URI=%s, Predicado URI=%s, Objeto URI=%s",
                uriSubject, uriPredicate, uriObject));

        // Comparación del sujeto
        if (subject != null && !subject.startsWith("?")) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);  // Usa el método correcto para sujeto
            if (s == null || s == -1) {
                System.out.println("Sujeto no encontrado o inválido: " + subject);
                return false;
            }
            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
            if (!s.equals(t.getSubject())) {
                System.out.println("Sujeto no coincide.");
                return false;
            }
        } else if (subject != null && subject.startsWith("?")) {
            System.out.println("El sujeto es una variable: " + subject);
        }

        // Comparación del predicado
        if (predicate != null && !predicate.startsWith("?")) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);  // Usa el método correcto para predicado
            if (p == null || p == -1) {
                System.out.println("Predicado no encontrado o inválido: " + predicate);
                return false;
            }
            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
            if (!p.equals(t.getPredicate())) {
                System.out.println("Predicado no coincide.");
                return false;
            }
        } else if (predicate != null && predicate.startsWith("?")) {
            System.out.println("El predicado es una variable: " + predicate);
        }

        // Comparación del objeto
        if (object != null && !object.startsWith("?")) {
            o = TripleIDConvert.stringToIDObject(dictionary, object);  // Usa el método correcto para objeto
            if (o == null || o == -1) {
                System.out.println("Objeto no encontrado o inválido: " + object);
                return false;
            }
            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
            if (!o.equals(t.getObject())) {
                System.out.println("Objeto no coincide.");
                return false;
            }
        } else if (object != null && object.startsWith("?")) {
            System.out.println("El objeto es una variable: " + object);
        }

        System.out.println("Triple coincide: " + t);
        return true;
    }

}
