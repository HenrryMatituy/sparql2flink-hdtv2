package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;

    // Mapas para almacenar las relaciones entre URIs y IDs
    private final Map<String, Integer> subjectMap = new HashMap<>();
    private final Map<String, Integer> predicateMap = new HashMap<>();
    private final Map<String, Integer> objectMap = new HashMap<>();
    private final Map<Integer, String> reverseSubjectMap = new HashMap<>();
    private final Map<Integer, String> reversePredicateMap = new HashMap<>();
    private final Map<Integer, String> reverseObjectMap = new HashMap<>();

    private static final Logger logger = Logger.getLogger(SerializableDictionary.class.getName());

    // Cargar las secciones del diccionario (sujetos, predicados, objetos) desde HDT
    public void loadFromHDTDictionary(Dictionary dictionary) {
        loadSection(dictionary, TripleComponentRole.SUBJECT);
        loadSection(dictionary, TripleComponentRole.PREDICATE);
        loadSection(dictionary, TripleComponentRole.OBJECT);
    }

    // Método para cargar cada sección del diccionario (sujeto, predicado, objeto)
    private void loadSection(Dictionary dictionary, TripleComponentRole role) {
        int numEntries = 0;
        Map<String, Integer> map = null;
        Map<Integer, String> reverseMap = null;

        // Asignar el mapa correspondiente según el rol
        switch (role) {
            case SUBJECT:
                numEntries = Math.toIntExact(dictionary.getNsubjects());
                map = subjectMap;
                reverseMap = reverseSubjectMap;
                logger.info("Cargando SUBJECTs: " + numEntries);
                break;
            case PREDICATE:
                numEntries = Math.toIntExact(dictionary.getNpredicates());
                map = predicateMap;
                reverseMap = reversePredicateMap;
                logger.info("Cargando PREDICATEs: " + numEntries);
                break;
            case OBJECT:
                numEntries = Math.toIntExact(dictionary.getNobjects());
                map = objectMap;
                reverseMap = reverseObjectMap;
                logger.info("Cargando OBJECTs: " + numEntries);
                break;
        }

        // Procesar cada entrada y asignar a los mapas
        for (int i = 1; i <= numEntries; i++) {
            String uri = (String) dictionary.idToString(i, role);
            if (uri != null && !uri.isEmpty()) {
                map.put(uri, i);
                reverseMap.put(i, uri);
                logger.info(String.format("ID=%d, Role=%s, URI=%s", i, role, uri));
            } else {
                logger.warning(String.format("URI vacía o nula para el ID=%d, Role=%s", i, role));
            }
        }
    }

    // Método para convertir una URI a su ID correspondiente según el rol (sujeto, predicado, objeto)
    public int stringToID(String value, TripleComponentRole role) {
        Integer id = null;
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
        }

        if (id == null) {
            logger.severe("stringToID: No ID found for value: " + value + ", role: " + role);
            return -1;
        }
        return id;
    }

    // Método para convertir un ID a su URI correspondiente según el rol (sujeto, predicado, objeto)
    public String idToString(int id, TripleComponentRole role) {
        String value = null;
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
