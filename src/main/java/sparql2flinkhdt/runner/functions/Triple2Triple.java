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
        System.out.println("Entrando al método filter de Triple2Triple (TRIPLE2TRIPLE)");

        // Variables para los IDs de sujeto, predicado y objeto
        Integer s = null, p = null, o = null;

        // Obtener los IDs actuales del triple
        long tSubject = t.getSubject();
        long tPredicate = t.getPredicate();
        long tObject = t.getObject();

        // Convertir los IDs a URIs para fines de depuración (opcional)
        String uriSubject = TripleIDConvert.idToString(dictionary, (int) tSubject, TripleComponentRole.SUBJECT);
        String uriPredicate = TripleIDConvert.idToString(dictionary, (int) tPredicate, TripleComponentRole.PREDICATE);
        String uriObject = TripleIDConvert.idToString(dictionary, (int) tObject, TripleComponentRole.OBJECT);

        System.out.println("URI del Sujeto: " + uriSubject);
        System.out.println("URI del Predicado: " + uriPredicate);
        System.out.println("URI del Objeto: " + uriObject);

        // Procesamiento del sujeto
        if (subject == null) {
            s = (int) tSubject;
        } else {
            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
            if (s == -1) {
                System.out.println("Error: Sujeto no encontrado en el diccionario para el URI: " + subject);
                return false;
            }
        }

        // Procesamiento del predicado
        if (predicate == null) {
            p = (int) tPredicate;
        } else {
            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
            if (p == -1) {
                System.out.println("Error: Predicado no encontrado en el diccionario para el URI: " + predicate);
                return false;
            }
        }

        // Procesamiento del objeto
        if (object == null) {
            o = (int) tObject;
        } else {
            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
            if (o == -1) {
                System.out.println("Error: Objeto no encontrado en el diccionario para el URI: " + object);
                return false;
            }
        }

        // Realizar las comparaciones
        boolean subjectMatches = (subject == null) || (tSubject == s.longValue());
        boolean predicateMatches = (predicate == null) || (tPredicate == p.longValue());
        boolean objectMatches = (object == null) || (tObject == o.longValue());

        // Depuración detallada
        System.out.println(String.format("Verificando triple: Sujeto=%d, Predicado=%d, Objeto=%d", tSubject, tPredicate, tObject));
        System.out.println(String.format("Comparando sujeto esperado=%s con sujeto encontrado=%d", s, tSubject));
        System.out.println(String.format("Comparando predicado esperado=%s con predicado encontrado=%d", p, tPredicate));
        System.out.println(String.format("Comparando objeto esperado=%s con objeto encontrado=%d", o, tObject));

        // Resultado final del filtro
        return subjectMatches && predicateMatches && objectMatches;
    }



}
