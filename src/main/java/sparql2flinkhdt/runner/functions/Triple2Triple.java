package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

public class Triple2Triple implements FilterFunction<TripleID> {

    private SerializableDictionary dictionary;
    private String subject;
    private String predicate;
    private String object;

    public Triple2Triple(SerializableDictionary dictionary, String subject, String predicate, String object) {
        this.dictionary = dictionary;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    @Override
    public boolean filter(TripleID t) {
        System.out.println("Entrando al método filter de Triple2Triple");

        // Variables para los IDs de sujeto, predicado y objeto
        Long s = null, p = null, o = null;

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
            s = tSubject;
        } else {
            Integer subjectId = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
            if (subjectId == -1) {
                System.out.println("Error: Sujeto no encontrado en el diccionario para el URI: " + subject);
                return false;
            }
            s = subjectId.longValue();
        }

        // Procesamiento del predicado
        if (predicate == null) {
            p = tPredicate;
        } else {
            Integer predicateId = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
            if (predicateId == -1) {
                System.out.println("Error: Predicado no encontrado en el diccionario para el URI: " + predicate);
                return false;
            }
            p = predicateId.longValue();
        }

        // Procesamiento del objeto
        if (object == null) {
            o = tObject;
        } else {
            Integer objectId = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
            if (objectId == -1) {
                System.out.println("Error: Objeto no encontrado en el diccionario para el URI: " + object);
                return false;
            }
            o = objectId.longValue();
        }

        // Realizar las comparaciones
        boolean subjectMatches = (subject == null) || (tSubject == s);
        boolean predicateMatches = (predicate == null) || (tPredicate == p);
        boolean objectMatches = (object == null) || (tObject == o);

        // Depuración detallada
        System.out.println(String.format("Verificando triple: Sujeto ID=%d, Predicado ID=%d, Objeto ID=%d", tSubject, tPredicate, tObject));
        System.out.println(String.format("Sujeto esperado ID=%s, Sujeto encontrado ID=%d", s, tSubject));
        System.out.println(String.format("Predicado esperado ID=%s, Predicado encontrado ID=%d", p, tPredicate));
        System.out.println(String.format("Objeto esperado ID=%s, Objeto encontrado ID=%d", o, tObject));
        System.out.println("Resultado de la comparación: Sujeto=" + subjectMatches + ", Predicado=" + predicateMatches + ", Objeto=" + objectMatches);

        // Resultado final del filtro
        return subjectMatches && predicateMatches && objectMatches;
    }
}
