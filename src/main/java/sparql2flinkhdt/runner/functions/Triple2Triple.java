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

        Integer s = null, p = null, o = null;

        String uriSubject = TripleIDConvert.idToString(dictionary, (int) t.getSubject(), TripleComponentRole.SUBJECT);
        System.out.println("URI del Sujeto (TRIPLE2TRIPLE): " + uriSubject);

        String uriPredicate = TripleIDConvert.idToString(dictionary, (int) t.getPredicate(), TripleComponentRole.PREDICATE);
        System.out.println("URI del PredicadoXX (TRIPLE2TRIPLE): " + uriPredicate);

        String uriObject = TripleIDConvert.idToString(dictionary, (int) t.getObject(), TripleComponentRole.OBJECT);
        System.out.println("URI del Objeto (TRIPLE2TRIPLE): " + uriObject);

        // Mensajes para indicar qué triple se está verificando
        System.out.println(String.format("Verificando triple (TRIPLE2TRIPLE): Subject=%d, Predicate=%d, Object=%d",
                t.getSubject(), t.getPredicate(), t.getObject()));

        // Verificar si el predicado es nulo antes de compararlo
        if (predicate != null) {
            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
            System.out.println("Esperado predicado: " + predicate + " -> ID: " + p);
        } else {
            System.out.println("Predicado esperado es null (TRIPLE2TRIPLE)");
            return false;
        }

        // Comprobación de si el valor `p` es `null` o no válido
        if (p == null || p == -1) {
            System.out.println("Error: Predicado esperado no fue encontrado en el diccionario (TRIPLE2TRIPLE)");
            return false;
        }

        // Imprimir el predicado encontrado para asegurar que estamos comparando los valores correctos
        System.out.println("Comparando predicado esperado=" + p + " con predicado encontrado=" + t.getPredicate());

        // Comparar los predicados y verificar si coinciden
        if (t.getPredicate() == p.longValue()) {
            System.out.println("Predicado coincide (TRIPLE2TRIPLE)");
            return true;
        } else {
            System.out.println(String.format("PredicadoXX no coincide (TRIPLE2TRIPLE): esperado predicado=%d, " +
                    "encontrado predicado=%d", p, t.getPredicate()));
            return false;
        }
         // Comparar los IDs según los valores proporcionados
//        if (subject == null && predicate != null && object != null) {
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//            System.out.println("Asignando ID para predicado: " + predicate + " -> ID: " + p);
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            // Verificar si alguno de los IDs no existe
//            if (p == -1 || o == -1) {
//                System.out.println("ID no encontrado para predicado u objeto (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            if (t.getPredicate() == p.longValue() && t.getObject() == o.longValue()) {
//                System.out.println("Predicado y objeto coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Predicado o objeto no coinciden (TRIPLE2TRIPLE): esperado predicado=%d, objeto=%d;" +
//                        " encontrado predicado=%d, objeto=%d", p, o, t.getPredicate(), t.getObject()));
//                return false;
//            }
//
//        } else if (subject != null && predicate == null && object != null) {
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            if (s == -1 || o == -1) {
//                System.out.println("ID no encontrado para sujeto u objeto (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            if (t.getSubject() == s.longValue() && t.getObject() == o.longValue()) {
//                System.out.println("Sujeto y objeto coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto o objeto no coinciden (TRIPLE2TRIPLE): esperado sujeto=%d, objeto=%d; " +
//                        "encontrado sujeto=%d, objeto=%d", s, o, t.getSubject(), t.getObject()));
//                return false;
//            }
//
//        } else if (subject != null && predicate != null && object == null) {
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//            System.out.println("Asignando ID para predicado: " + predicate + " -> ID: " + p);
//            if (s == -1 || p == -1) {
//                System.out.println("ID no encontrado para sujeto o predicado (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            if (t.getSubject() == s.longValue() && t.getPredicate() == p.longValue()) {
//                System.out.println("Sujeto y predicado coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto o predicado no coinciden (TRIPLE2TRIPLE): esperado sujeto=%d, predicado=%d;" +
//                        " encontrado sujeto=%d, predicado=%d", s, p, t.getSubject(), t.getPredicate()));
//                return false;
//            }
//
//        } else if (subject != null && predicate == null && object == null) {
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//
//            if (s == -1) {
//                System.out.println("ID no encontrado para sujeto (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            if (t.getSubject() == s.longValue()) {
//                System.out.println("Sujeto coincide (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto no coincide (TRIPLE2TRIPLE): esperado sujeto=%d, encontrado sujeto=%d", s,
//                        t.getSubject()));
//                return false;
//            }
//
//        } else if (subject == null && predicate != null && object == null) {
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//            System.out.println("Asignando ID para predicado: " + predicate + " -> ID: " + p);
//            if (p == -1) {
//                System.out.println("ID no encontrado para predicado (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            System.out.println(String.format("Comparando predicados: esperado predicado=%d, encontrado predicado=%d", p, t.getPredicate()));
//
//            if (t.getPredicate() == p.longValue()) {
//                System.out.println("Predicado coincide (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Predicado no coincide (TRIPLE2TRIPLE): esperado predicado=%d, encontrado " +
//                        "predicado=%d", p, t.getPredicate()));
//                return false;
//            }
//        }
//        else if (subject == null && predicate == null && object != null) {
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            if (o == -1) {
//                System.out.println("ID no encontrado para objeto (TRIPLE2TRIPLE)");
//                return false;
//            }
//
//            if (t.getObject() == o.longValue()) {
//                System.out.println("Objeto coincide");
//                return true;
//            } else {
//                System.out.println(String.format("Objeto no coincide: esperado objeto=%d, encontrado objeto=%d", o, t.getObject()));
//                return false;
//            }
//
//        } else {
//            // Si no hay filtros definidos, acepta todos los triples
//            System.out.println("No hay filtros, triple aceptado");
//            return true;
//        }
    }


//    @Override
//    public boolean filter(TripleID t) {
////        Integer s = null, p = null, o = null;
//        System.out.println("Entrando al método filter de Triple2Triple (TRIPLE2TRIPLE)");
//
//        Integer s = null, p = null, o = null;
//
//        String uriSubject = TripleIDConvert.idToString(dictionary, (int) t.getSubject(), TripleComponentRole.SUBJECT);
//        System.out.println("URI del Sujeto (TRIPLE2TRIPLE): " + uriSubject);
//
//        String uriPredicate = TripleIDConvert.idToString(dictionary, (int) t.getPredicate(), TripleComponentRole.PREDICATE);
//        System.out.println("URI del Predicado (TRIPLE2TRIPLE): " + uriPredicate);
//
//        String uriObject = TripleIDConvert.idToString(dictionary, (int) t.getObject(), TripleComponentRole.OBJECT);
//        System.out.println("URI del Objeto (TRIPLE2TRIPLE): " + uriObject);
//
//
//
//        // Mensajes para indicar qué triple se está verificando
//        System.out.println(String.format("Verificando triple (TRIPLE2TRIPLE): Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//
//        // Comparar los IDs según los valores proporcionados
//        if (subject == null && predicate != null && object != null) {
//            // Filtrado solo por predicado y objeto
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            if (t.getPredicate() == p.longValue() && t.getObject() == o.longValue()) {
//                System.out.println("Predicado y objeto coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Predicado o objeto no coinciden (TRIPLE2TRIPLE): esperado predicado=%d, objeto=%d;" +
//                        " encontrado predicado=%d, objeto=%d", p, o, t.getPredicate(), t.getObject()));
//                return false;
//            }
//
//        } else if (subject != null && predicate == null && object != null) {
//            // Filtrado por sujeto y objeto
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            if (t.getSubject() == s.longValue() && t.getObject() == o.longValue()) {
//                System.out.println("Sujeto y objeto coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto o objeto no coinciden (TRIPLE2TRIPLE): esperado sujeto=%d, objeto=%d; " +
//                        "encontrado sujeto=%d, objeto=%d", s, o, t.getSubject(), t.getObject()));
//                return false;
//            }
//
//        } else if (subject != null && predicate != null && object == null) {
//            // Filtrado por sujeto y predicado
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//
//            if (t.getSubject() == s.longValue() && t.getPredicate() == p.longValue()) {
//                System.out.println("Sujeto y predicado coinciden (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto o predicado no coinciden (TRIPLE2TRIPLE): esperado sujeto=%d, predicado=%d;" +
//                        " encontrado sujeto=%d, predicado=%d", s, p, t.getSubject(), t.getPredicate()));
//                return false;
//            }
//
//        } else if (subject != null && predicate == null && object == null) {
//            // Filtrado solo por sujeto
//            s = TripleIDConvert.stringToID(dictionary, subject, TripleComponentRole.SUBJECT);
//
//            if (t.getSubject() == s.longValue()) {
//                System.out.println("Sujeto coincide (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Sujeto no coincide (TRIPLE2TRIPLE): esperado sujeto=%d, encontrado sujeto=%d", s,
//                        t.getSubject()));
//                return false;
//            }
//
//        } else if (subject == null && predicate != null && object == null) {
//            // Filtrado solo por predicado
//            p = TripleIDConvert.stringToID(dictionary, predicate, TripleComponentRole.PREDICATE);
//
//            if (t.getPredicate() == p.longValue()) {
//                System.out.println("Predicado coincide (TRIPLE2TRIPLE)");
//                return true;
//            } else {
//                System.out.println(String.format("Predicado no coincide (TRIPLE2TRIPLE): esperado predicado=%d, encontrado " +
//                        "predicado=%d", p, t.getPredicate()));
//                return false;
//            }
//
//        } else if (subject == null && predicate == null && object != null) {
//            // Filtrado solo por objeto
//            o = TripleIDConvert.stringToID(dictionary, object, TripleComponentRole.OBJECT);
//
//            if (t.getObject() == o.longValue()) {
//                System.out.println("Objeto coincide");
//                return true;
//            } else {
//                System.out.println(String.format("Objeto no coincide: esperado objeto=%d, encontrado objeto=%d", o, t.getObject()));
//                return false;
//            }
//
//        } else {
//            // Si no hay filtros definidos, acepta todos los triples
//            System.out.println("No hay filtros, triple aceptado");
//            return true;
//        }
//    }
}
