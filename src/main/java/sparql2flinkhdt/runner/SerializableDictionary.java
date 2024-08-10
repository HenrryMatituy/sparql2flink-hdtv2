package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.dictionary.Dictionary;
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

    public void loadFromHDTDictionary(Dictionary dictionary) {
        loadSection(dictionary.getSubjects(), TripleComponentRole.SUBJECT);
        loadSection(dictionary.getPredicates(), TripleComponentRole.PREDICATE);
        loadSection(dictionary.getObjects(), TripleComponentRole.OBJECT);
    }

    private void loadSection(Iterable<String> section, TripleComponentRole role) {
        int id = 1; // Asumimos que los IDs empiezan en 1, ajusta según sea necesario
        for (String uri : section) {
            switch (role) {
                case SUBJECT:
                    subjectMap.put(uri, id);
                    reverseSubjectMap.put(id, uri);
                    break;
                case PREDICATE:
                    predicateMap.put(uri, id);
                    reversePredicateMap.put(id, uri);
                    break;
                case OBJECT:
                    objectMap.put(uri, id);
                    reverseObjectMap.put(id, uri);
                    break;
            }
            id++;
        }
    }

    public int stringToID(String value, TripleComponentRole role) {
        Integer id;
        switch (role) {
            case SUBJECT:
                id = subjectMap.get(value);
                break;
            case PREDICATE:
                id = predicateMap.get(value);
                break;
            case OBJECT:
                id = objectMap.get(value);
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
        if (id == null) {
            logger.severe("stringToID: No ID found for value: " + value + ", role: " + role);
            return -1;
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
        if (value == null) {
            logger.severe("idToString: No value found for id: " + id + ", role: " + role);
            return null;
        }
        logger.info(String.format("idToString: id=%d, role=%s, value=%s", id, role, value));
        return value;
    }

    @Override
    public String toString() {
        return "SerializableDictionary{...}";
    }
}
