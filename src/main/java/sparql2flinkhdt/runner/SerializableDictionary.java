package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Integer> subjectMap = new HashMap<>();
    private final Map<String, Integer> predicateMap = new HashMap<>();
    private final Map<String, Integer> objectMap = new HashMap<>();
    private final Map<Integer, String> reverseSubjectMap = new HashMap<>();
    private final Map<Integer, String> reversePredicateMap = new HashMap<>();
    private final Map<Integer, String> reverseObjectMap = new HashMap<>();

    public SerializableDictionary() {
        // Populate these maps based on your actual dictionary
        // For example, you might iterate over the entries in your existing Dictionary
        // and fill these maps accordingly
    }

    public int stringToID(String value, TripleComponentRole role) {
        switch (role) {
            case SUBJECT:
                return subjectMap.getOrDefault(value, -1);
            case PREDICATE:
                return predicateMap.getOrDefault(value, -1);
            case OBJECT:
                return objectMap.getOrDefault(value, -1);
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
    }

    public String idToString(int id, TripleComponentRole role) {
        switch (role) {
            case SUBJECT:
                return reverseSubjectMap.get(id);
            case PREDICATE:
                return reversePredicateMap.get(id);
            case OBJECT:
                return reverseObjectMap.get(id);
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
    }
    public String toString() {
        // Implementar un método toString adecuado
        return "SerializableDictionary{...}";
    }
}



//package sparql2flinkhdt.runner;
//
//import org.rdfhdt.hdt.dictionary.Dictionary;
//import org.rdfhdt.hdt.enums.TripleComponentRole;
//
//import java.io.Serializable;
//
//public class SerializableDictionary implements Serializable {
//    private static final long serialVersionUID = 1L;
//    private final Dictionary dictionary;
//
//    public SerializableDictionary(Dictionary dictionary) {
//        this.dictionary = dictionary;
//    }
//
//    public int stringToID(String value, TripleComponentRole role) {
//        return (int) dictionary.stringToId(value, role);
//    }
//
//    public String idToString(int id, TripleComponentRole role) {
//        return (String) dictionary.idToString(id, role);
//    }
//}



//
//import org.rdfhdt.hdt.dictionary.Dictionary;
//import org.rdfhdt.hdt.enums.RDFNotation;
//
//import java.io.Serializable;
//
//public class SerializableDictionary implements Serializable {
//    private transient Dictionary dictionary;
//
//    public SerializableDictionary(Dictionary dictionary) {
//        this.dictionary = dictionary;
//    }
//
//    // Methods for conversion
//    public Integer stringToIDSubject(String subject) {
//        return (int) dictionary.stringToId(subject,);
//    }
//
//    public Integer stringToIDPredicate(String predicate) {
//        return (int) dictionary.stringToId(predicate, RDFNodeType.PREDICATE);
//    }
//
//    public Integer stringToIDObject(String object) {
//        return (int) dictionary.stringToId(object, RDFNodeType.OBJECT);
//    }
//
//    // Getters and setters for the dictionary field
//    public Dictionary getDictionary() {
//        return dictionary;
//    }
//
//    public void setDictionary(Dictionary dictionary) {
//        this.dictionary = dictionary;
//    }
//}
