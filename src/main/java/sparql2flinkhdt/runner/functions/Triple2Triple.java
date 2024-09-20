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
//        Integer s, p, o;
//        System.out.println("Evaluandoooo triple: Subject=" + t.getSubject() + ", Predicate=" + t.getPredicate() + ", " +
//                "Object=" + t.getObject());
//        System.out.println("Verificandoooo mapeo de Sujeto: " + subject + " a ID: " + t.getSubject());
//        System.out.println("Verificandoooo mapeo de Predicado: " + predicate + " a ID: " + t.getPredicate());
//        System.out.println("Verificandoooo mapeo de Objeto: " + object + " a ID: " + t.getObject());
//

        Integer s = TripleIDConvert.stringToIDSubject(dictionary, subject);
        Integer p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
        Integer o = TripleIDConvert.stringToIDObject(dictionary, object);

        System.out.println("Verificandoyyy triple: Subject=" + s + ", Predicate=" + p + ", Object=" + o);


        // Evaluar si los valores no son nulos
        if (subject != null) {
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            if (t.getSubject() != s) {
                return false;
            }
        }

        if (predicate != null) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            if (t.getPredicate() != p) {
                return false;
            }
        }

        if (object != null) {
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            if (t.getObject() != o) {
                return false;
            }
        }
        return true;
    }


//    public boolean filter(TripleID t) {
//        Integer s, p, o;
//        System.out.println("Evaluando triple: Subject=" + t.getSubject() + ", Predicate=" + t.getPredicate() + ", Object=" + t.getObject());
//        System.out.println("Verificando mapeo de Sujeto: " + subject + " a ID: " + t.getSubject());
//        System.out.println("Verificando mapeo de Predicado: " + predicate + " a ID: " + t.getPredicate());
//        System.out.println("Verificando mapeo de Objeto: " + object + " a ID: " + t.getObject());
//
//        if (subject == null && predicate != null && object != null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Sujeto: " + t.toString());
//            return (p.equals(t.getPredicate()) && o.equals(t.getObject()));
//        } else if (subject != null && predicate == null && object != null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Predicado: " + t.toString());
//            return (s.equals(t.getSubject()) && o.equals(t.getObject()));
//        } else if (subject != null && predicate != null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            System.out.println("Triple con Objeto: " + t.toString());
//            return (s.equals(t.getSubject()) && p.equals(t.getPredicate()));
//        } else if (subject != null && predicate == null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            System.out.println("Triple con Predicado y Objeto: " + t.toString());
//            return s.equals(t.getSubject());
//        } else if (subject == null && predicate != null && object == null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            System.out.println("Triple con Sujeto y Objeto: " + t.toString());
//            return p.equals(t.getPredicate());
//        } else if (subject == null && predicate == null && object != null) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Sujeto y Predicado: " + t.toString());
//            return o.equals(t.getObject());
//        } else {
//            return true;
//        }
//    }


//    public boolean filter(TripleID t) {
//        Integer s, p, o;
//        System.out.println("Evaluando triple: Subject=" + t.getSubject() + ", Predicate=" + t.getPredicate() + ", Object=" + t.getObject());
//                if (subject == null && predicate != null && object != null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Sujeto: " + t.toString());
//            return (t.getPredicate() == p && t.getObject() == o);
//        } else if (subject != null && predicate == null && object != null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//                    System.out.println("Triple con Predicado: " + t.toString());
//            return (t.getSubject() == s && t.getObject() == o);
//        } else if (subject != null && predicate != null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//                    System.out.println("Triple con Objeto: " + t.toString());
//            return (t.getSubject() == s && t.getPredicate() == p);
//                    } else if (subject != null && predicate == null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//                    System.out.println("Triple con Prediado y Objeto: " + t.toString());
//            return t.getSubject() == s;
//        } else if (subject == null && predicate != null && object == null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//                    System.out.println("Triple con Sujeto y Objeto: " + t.toString());
//            return t.getPredicate() == p;
//        } else if (subject == null && predicate == null && object != null) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//                    System.out.println("Triple con Sujeto y Predicado: " + t.toString());
//            return t.getObject() == o;
//        } else {
//                    System.out.println("Triple con Sujeto, Predicado y Objeto: " + t.toString());
//            return true;
//        }
//    }

//    public boolean filter(TripleID t) {
//        Integer s, p, o;
//        System.out.println("Evaluando triple: Subject=" + t.getSubject() + ", Predicate=" + t.getPredicate() + ", Object=" + t.getObject());
//        System.out.println("Verificando mapeo de Sujeto: " + predicate + " a ID: " + t.getSubject());
//        System.out.println("Verificando mapeo de Predicado: " + predicate + " a ID: " + t.getPredicate());
//        System.out.println("Verificando mapeo de Objeto: " + predicate + " a ID: " + t.getObject());
//
//        if (subject == null && predicate != null && object != null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Sujeto: " + t.toString());
//            return (t.getPredicate() == p && t.getObject() == o);
//        } else if (subject != null && predicate == null && object != null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Predicado: " + t.toString());
//            return (t.getSubject() == s && t.getObject() == o);
//        } else if (subject != null && predicate != null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            System.out.println("Triple con Objeto: " + t.toString());
//            return (t.getSubject() == s && t.getPredicate() == p);
//        } else if (subject != null && predicate == null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            System.out.println("Triple con Predicado y Objeto: " + t.toString());
//            return t.getSubject() == s;
//        } else if (subject == null && predicate != null && object == null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            System.out.println("Triple con Sujeto y Objeto: " + t.toString());
//            return t.getPredicate() == p;
//        } else if (subject == null && predicate == null && object != null) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            System.out.println("Triple con Sujeto y Predicado: " + t.toString());
//            return t.getObject() == o;
//        } else {
//            return true;
//        }
//    }

}
