package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Integer> subjectMap = new HashMap<>();
    private final Map<String, Integer> predicateMap = new HashMap<>();
    private final Map<String, Integer> objectMap = new HashMap<>();
    private final Map<Integer, String> reverseSubjectMap = new HashMap<>();
    private final Map<Integer, String> reversePredicateMap = new HashMap<>();
    private final Map<Integer, String> reverseObjectMap = new HashMap<>();

    private static final Logger logger = Logger.getLogger(SerializableDictionary.class.getName());

    public SerializableDictionary() {
        // Initialize with hypothetical values
        subjectMap.put("http://example.org/subject1", 1);
        predicateMap.put("http://xmlns.com/foaf/0.1/name", 2);
        predicateMap.put("http://xmlns.com/foaf/0.1/mbox", 3);
        objectMap.put("http://example.org/object1", 4);

        reverseSubjectMap.put(1, "http://example.org/subject1");
        reversePredicateMap.put(2, "http://xmlns.com/foaf/0.1/name");
        reversePredicateMap.put(3, "http://xmlns.com/foaf/0.1/mbox");
        reverseObjectMap.put(4, "http://example.org/object1");
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
        logger.info(String.format("stringToID: value=%s, role=%s, id=%d", value, role, id));
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
        logger.info(String.format("idToString: id=%d, role=%s, value=%s", id, role, value));
        return value;
    }

    @Override
    public String toString() {
        return "SerializableDictionary{...}";
    }
}
