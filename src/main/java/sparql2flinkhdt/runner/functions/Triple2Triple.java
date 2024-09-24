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
//    public boolean filter(TripleID t) {
//        // Verificación de que el diccionario está presente
//        if (dictionary != null) {
//            System.out.println("Diccionario disponible en filter: " + dictionary);
//        } else {
//            System.out.println("El diccionario es nulo en filter");
//            return false;
//        }
//
//        Integer s = null, p = null, o = null;
//
//        // Casos donde sólo tenemos el predicado y el objeto
//        if (subject == null && predicate != null && object != null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//
//            // Verificación del predicado y objeto
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado o inválido: " + object);
//                return false;
//            }
//
//            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//
//            if (t.getPredicate() != p || t.getObject() != o) {
//                System.out.println("Predicado o objeto no coincide.");
//                return false;
//            }
//            return true;
//
//            // Casos donde sólo tenemos el sujeto y el objeto
//        } else if (subject != null && predicate == null && object != null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//
//            // Verificación del sujeto y objeto
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado o inválido: " + subject);
//                return false;
//            }
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado o inválido: " + object);
//                return false;
//            }
//
//            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//
//            if (t.getSubject() != s || t.getObject() != o) {
//                System.out.println("Sujeto o objeto no coincide.");
//                return false;
//            }
//            return true;
//
//            // Casos donde sólo tenemos el sujeto y el predicado
//        } else if (subject != null && predicate != null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//
//            // Verificación del sujeto y predicado
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado o inválido: " + subject);
//                return false;
//            }
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
//
//            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//
//            if (t.getSubject() != s || t.getPredicate() != p) {
//                System.out.println("Sujeto o predicado no coincide.");
//                return false;
//            }
//            return true;
//
//            // Casos donde sólo tenemos el sujeto
//        } else if (subject != null && predicate == null && object == null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//
//            // Verificación del sujeto
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado o inválido: " + subject);
//                return false;
//            }
//
//            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//
//            if (t.getSubject() != s) {
//                System.out.println("Sujeto no coincide.");
//                return false;
//            }
//            return true;
//
//            // Casos donde sólo tenemos el predicado
//        } else if (subject == null && predicate != null && object == null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//
//            // Verificación del predicado
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
//
//            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//
//            if (t.getPredicate() != p) {
//                System.out.println("Predicado no coincide.");
//                return false;
//            }
//            return true;
//
//            // Casos donde sólo tenemos el objeto
//        } else if (subject == null && predicate == null && object != null) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//
//            // Verificación del objeto
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado o inválido: " + object);
//                return false;
//            }
//
//            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//
//            if (t.getObject() != o) {
//                System.out.println("Objeto no coincide.");
//                return false;
//            }
//            return true;
//
//            // Si no hay ningún criterio de filtro, se acepta cualquier triple
//        } else {
//            return true;
//        }
//    }

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
            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
            if (s == null || s == -1) {
                System.out.println("Sujeto no encontrado o inválido: " + subject);
                return false;
            }
            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
            if (s.intValue() != t.getSubject()) {
                System.out.println("Sujeto no coincide.");
                return false;
            }
        } else if (subject != null && subject.startsWith("?")) {
            System.out.println("El sujeto es una variable: " + subject);
        }

        // Comparación del predicado
        if (predicate != null && !predicate.startsWith("?")) {
            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
            if (p == null || p == -1) {
                System.out.println("Predicado no encontrado o inválido: " + predicate);
                return false;
            }
            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
            if (p.intValue() != t.getPredicate()) {
                System.out.println(String.format("Predicado no coincide: esperado %d, encontrado %d", p.intValue(), t.getPredicate()));
                return false;
            }
        } else if (predicate != null && predicate.startsWith("?")) {
            System.out.println("El predicado es una variable: " + predicate);
        }

        // Comparación del objeto
        if (object != null && !object.startsWith("?")) {
            o = TripleIDConvert.stringToIDObject(dictionary, object);
            if (o == null || o == -1) {
                System.out.println("Objeto no encontrado o inválido: " + object);
                return false;
            }
            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
            if (o.intValue() != t.getObject()) {
                System.out.println("Objeto no coincide.");
                return false;
            }
        } else if (object != null && object.startsWith("?")) {
            System.out.println("El objeto es una variable: " + object);
        }

        System.out.println("Triple coincide: " + t);
        return true;
    }



    //El que mejor funciona
//    public boolean filter(TripleID t) {
//        Integer s = null, p = null, o = null;
//
//        // Mapear IDs a URIs
//        String uriSubject = dictionary.idToString((int) t.getSubject(), TripleComponentRole.SUBJECT);
//        String uriPredicate = dictionary.idToString((int) t.getPredicate(), TripleComponentRole.PREDICATE);
//        String uriObject = dictionary.idToString((int) t.getObject(), TripleComponentRole.OBJECT);
//
//        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//        System.out.println(String.format("Triple actual: Sujeto URI=%s, Predicado URI=%s, Objeto URI=%s",
//                uriSubject, uriPredicate, uriObject));
//
//        // Comparación del sujeto
//        if (subject != null && !subject.startsWith("?")) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado o inválido: " + subject);
//                return false;
//            }
//            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//            if (s.intValue() != t.getSubject()) {
//                System.out.println("Sujeto no coincide.");
//                return false;
//            }
//        } else if (subject != null && subject.startsWith("?")) {
//            System.out.println("El sujeto es una variable: " + subject);
//        }
//
//        // Comparación del predicado
//        if (predicate != null && !predicate.startsWith("?")) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
////            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//            if (p.intValue() != t.getPredicate()) {
////                System.out.println("Predicado no coincide.");
//                System.out.println(String.format("Predicado no coincide: esperado %d (URI: %s), encontrado %d (URI: %s)",
//                        p.intValue(), predicate, t.getPredicate(), uriPredicate));
//                return false;
//            }
//        } else if (predicate != null && predicate.startsWith("?")) {
//            System.out.println("El predicado es una variable: " + predicate);
//        }
//
//        // Comparación del objeto
//        if (object != null && !object.startsWith("?")) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado o inválido: " + object);
//                return false;
//            }
//            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//            if (o.intValue() != t.getObject()) {
//                System.out.println("Objeto no coincide.");
//                return false;
//            }
//        } else if (object != null && object.startsWith("?")) {
//            System.out.println("El objeto es una variable: " + object);
//        }
//
//          System.out.println("Triple coincide: " + t);
//        return true;
//    }

//    public boolean filter(TripleID t) {
//        Integer s = null, p = null, o = null;
//
//        // Mapear IDs a URIs
//        String uriSubject = dictionary.idToString((int) t.getSubject(), TripleComponentRole.SUBJECT);
//        String uriPredicate = dictionary.idToString((int) t.getPredicate(), TripleComponentRole.PREDICATE);
//        String uriObject = dictionary.idToString((int) t.getObject(), TripleComponentRole.OBJECT);
//
//        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//        System.out.println(String.format("Triple actual: Sujeto URI=%s, Predicado URI=%s, Objeto URI=%s",
//                uriSubject, uriPredicate, uriObject));
//
//        // Comparación del predicado
//        if (predicate != null && !predicate.startsWith("?")) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
//            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//            if (p.intValue() != t.getPredicate()) {
//                System.out.println("Predicado no coincide.");
//                return false;
//            }
//        }
//
//        // Similar para sujeto y objeto
//        // ...
//
//        // Si todo coincide
//        System.out.println("Triple coincide: " + t);
//        return true;
//    }
//
//    public boolean filteraa(TripleID t) {
//        Integer s = null, p = null, o = null;
//
//        // Log para depuración: muestra el triple original
//        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//
//        // Mapear IDs a URIs
//        String uriSubject = dictionary.idToString((int) t.getSubject(), TripleComponentRole.SUBJECT);
//        String uriPredicate = dictionary.idToString((int) t.getPredicate(), TripleComponentRole.PREDICATE);
//        String uriObject = dictionary.idToString((int) t.getObject(), TripleComponentRole.OBJECT);
//
//        System.out.println(String.format("Triple actual: Sujeto URI=%s, Predicado URI=%s, Objeto URI=%s",
//                uriSubject, uriPredicate, uriObject));
//
//        // Si el sujeto no es una variable (es decir, no es ?person)
//        if (subject != null) {
//            if (subject.startsWith("?")) {
//                System.out.println("El sujeto es una variable: " + subject);
//            } else {
//                s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//                if (s == null || s == -1) {
//                    System.out.println("Sujeto no encontrado o inválido: " + subject);
//                    return false;
//                }
//                System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//                if (!s.equals(t.getSubject())) {
//                    System.out.println("Sujeto no coincide.");
//                    return false;
//                }
//            }
//        }
//
//        // Si el predicado no es una variable (es decir, no es ?predicate)
//        if (predicate != null) {
//            if (predicate.startsWith("?")) {
//                System.out.println("El predicado es una variable: " + predicate);
//            } else {
//                p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//                if (p == null || p == -1) {
//                    System.out.println("Predicado no encontrado o inválido: " + predicate);
//                    return false;
//                }
//                System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//                if (!p.equals(t.getPredicate())) {
//                    System.out.println("Predicado no coincide.");
//                    return false;
//                }
//            }
//        }
//
//        // Si el objeto no es una variable (es decir, no es ?object)
//        if (object != null) {
//            if (object.startsWith("?")) {
//                System.out.println("El objeto es una variable: " + object);
//            } else {
//                o = TripleIDConvert.stringToIDObject(dictionary, object);
//                if (o == null || o == -1) {
//                    System.out.println("Objeto no encontrado o inválido: " + object);
//                    return false;
//                }
//                System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//                if (!o.equals(t.getObject())) {
//                    System.out.println("Objeto no coincide.");
//                    return false;
//                }
//            }
//        }
//
//        // Si todo coincide, mostrar un mensaje de éxito y retornar true
//        System.out.println("Triple coincide: " + t);
//        return true;
//    }

//    public boolean filter(TripleID t) {
//        Integer s = null, p = null, o = null;
//
//        // Log para depuración: muestra el triple original
//        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//
//        // Si el sujeto no es una variable (es decir, no es ?person)
//        if (subject != null && !subject.startsWith("?")) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado o inválido: " + subject);
//                return false;
//            }
//            System.out.println(String.format("Sujeto esperado: %s (ID: %d), Sujeto en triple: %d", subject, s, t.getSubject()));
//            if (!s.equals(t.getSubject())) {
//                System.out.println("Sujeto no coincide.");
//                return false;
//            }
//        }
//
//        // Si el predicado no es una variable (es decir, no es ?predicate)
//        if (predicate != null && !predicate.startsWith("?")) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado o inválido: " + predicate);
//                return false;
//            }
//            System.out.println(String.format("Predicado esperado: %s (ID: %d), Predicado en triple: %d", predicate, p, t.getPredicate()));
//            if (!p.equals(t.getPredicate())) {
//                System.out.println("Predicado no coincide.");
//                return false;
//            }
//        }
//
//        // Si el objeto no es una variable (es decir, no es ?object)
//        if (object != null && !object.startsWith("?")) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado o inválido: " + object);
//                return false;
//            }
//            System.out.println(String.format("Objeto esperado: %s (ID: %d), Objeto en triple: %d", object, o, t.getObject()));
//            if (!o.equals(t.getObject())) {
//                System.out.println("Objeto no coincide.");
//                return false;
//            }
//        }
//
//        // Si todo coincide, mostrar un mensaje de éxito y retornar true
//        System.out.println("Triple coincide: " + t);
//        return true;
//    }

//    public boolean filter(TripleID t) {
//        Integer s = null, p = null, o = null;
//
//        // Log para depuración: muestra el triple original
//        System.out.println(String.format("Verificando triple: Subject=%d, Predicate=%d, Object=%d",
//                t.getSubject(), t.getPredicate(), t.getObject()));
//
//        // Si el sujeto no es una variable (es decir, no es ?person)
//        if (subject != null && !subject.startsWith("?")) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            if (s == null || s == -1) {
//                System.out.println("Sujeto no encontrado en el diccionario: " + subject);
//                return false;
//            }
//            System.out.println("Sujeto mapeado a ID: " + s);
//            if (!s.equals(t.getSubject())) {
//                return false;
//            }
//        }
//
//        // Si el predicado no es una variable (es decir, no es ?predicate)
//        if (predicate != null && !predicate.startsWith("?")) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (p == null || p == -1) {
//                System.out.println("Predicado no encontrado en el diccionario: " + predicate);
//                return false;
//            }
//            if (!p.equals(t.getPredicate())) {
//                return false;
//            }
//        }
//
//        // Si el objeto no es una variable (es decir, no es ?object)
//        if (object != null && !object.startsWith("?")) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            if (o == null || o == -1) {
//                System.out.println("Objeto no encontrado en el diccionario: " + object);
//                return false;
//            }
//            System.out.println("Objeto mapeado a ID: " + o);
//            if (!o.equals(t.getObject())) {
//                return false;
//            }
//        }
//
//        // Log para depuración: muestra los valores que fueron mapeados
//        System.out.println(String.format("Comparando triple con IDs mapeados: Sujeto=%d, Predicado=%d, Objeto=%d",
//                s, p, o));
//
//        // Si todos los filtros pasaron, retorna true
//        return true;
//    }



//    public boolean filter(TripleID t) {
//        Integer s = null, p = null, o = null;
////       System.out.println("Verificandoyyy triple: Subject=" + s + ", Predicate=" + p + ", Object=" + o);
//        System.out.println(String.format("Verificandoyyy triple Subject=%d, Predicate=%d, Object=%d", t.getSubject(), t.getPredicate(), t.getObject()));
//
//        // Si el sujeto no es una variable (es decir, no es ?person)
//        if (subject != null && !subject.startsWith("?")) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            if (s == null || s == -1 || !s.equals(t.getSubject())) {
//                return false;
//            }
//        }
//
//        // Si el predicado no es una variable
//        if (predicate != null && !predicate.startsWith("?")) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (p == null || p == -1 || !p.equals(t.getPredicate())) {
//                return false;
//            }
//        }
//
//        // Si el objeto no es una variable
//        if (object != null && !object.startsWith("?")) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            if (o == null || o == -1 || !o.equals(t.getObject())) {
//                return false;
//            }
//        }
//
//        return true; // Si todos los filtros pasaron
//    }
//
//

//    public boolean filter(TripleID t) {
////        Integer s, p, o;
////        System.out.println("Evaluandoooo triple: Subject=" + t.getSubject() + ", Predicate=" + t.getPredicate() + ", " +
////                "Object=" + t.getObject());
////        System.out.println("Verificandoooo mapeo de Sujeto: " + subject + " a ID: " + t.getSubject());
////        System.out.println("Verificandoooo mapeo de Predicado: " + predicate + " a ID: " + t.getPredicate());
////        System.out.println("Verificandoooo mapeo de Objeto: " + object + " a ID: " + t.getObject());
////
//
//        Integer s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//        Integer p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//        Integer o = TripleIDConvert.stringToIDObject(dictionary, object);
//
//        System.out.println("Verificandoyyy triple: Subject=" + s + ", Predicate=" + p + ", Object=" + o);
//
//
//        // Evaluar si los valores no son nulos
//        if (subject != null) {
//            s = TripleIDConvert.stringToIDSubject(dictionary, subject);
//            if (t.getSubject() != s) {
//                return false;
//            }
//        }
//
//        if (predicate != null) {
//            p = TripleIDConvert.stringToIDPredicate(dictionary, predicate);
//            if (t.getPredicate() != p) {
//                return false;
//            }
//        }
//
//        if (object != null) {
//            o = TripleIDConvert.stringToIDObject(dictionary, object);
//            if (t.getObject() != o) {
//                return false;
//            }
//        }
//        return true;
//    }


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
