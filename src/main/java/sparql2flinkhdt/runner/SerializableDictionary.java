package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(SerializableDictionary.class.getName());

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
        populateMaps();
    }

    private void populateMaps() {
        // Example population of the maps
        subjectMap.put("http://example.org/subject1", 1);
        predicateMap.put("http://xmlns.com/foaf/0.1/name", 2);
        objectMap.put("http://example.org/object1", 3);

        reverseSubjectMap.put(1, "http://example.org/subject1");
        reversePredicateMap.put(2, "http://xmlns.com/foaf/0.1/name");
        reverseObjectMap.put(3, "http://example.org/object1");
    }

    public int stringToID(String value, TripleComponentRole role) {
        int id;
        switch (role) {
            case SUBJECT:
                id = subjectMap.getOrDefault(value, -1);
                break;
            case PREDICATE:
                id = predicateMap.getOrDefault(value, -1);
                break;
            case OBJECT:
                id = objectMap.getOrDefault(value, -1);
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
        logger.info("stringToID: value=" + value + ", role=" + role + ", id=" + id);
        return id;
    }

    public String idToString(int id, TripleComponentRole role) {
        String value;
        switch (role) {
            case SUBJECT:
                value = reverseSubjectMap.get(id);
                break;
            case PREDICATE:
                value = reversePredicateMap.get(id);
                break;
            case OBJECT:
                value = reverseObjectMap.get(id);
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
        logger.info("idToString: id=" + id + ", role=" + role + ", value=" + value);
        return value;
    }

    @Override
    public String toString() {
        return "SerializableDictionary{" +
                "subjectMap=" + subjectMap +
                ", predicateMap=" + predicateMap +
                ", objectMap=" + objectMap +
                ", reverseSubjectMap=" + reverseSubjectMap +
                ", reversePredicateMap=" + reversePredicateMap +
                ", reverseObjectMap=" + reverseObjectMap +
                '}';
    }
}
