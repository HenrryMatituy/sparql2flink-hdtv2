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
        // Verificar si el predicado mbox se cargó correctamente
        Integer mboxID = predicateMap.get("http://xmlns.com/foaf/0.1/mbox");
        System.out.println("Verificando mbox en diccionario: " + mboxID);
    }

    // Metodo para cargar cada sección del diccionario (sujeto, predicado, objeto)
    private void loadSection(Dictionary dictionary, TripleComponentRole role) {
        int numEntries = 0;
        Map<String, Integer> map = null;
        Map<Integer, String> reverseMap = null;

        // Identificar el rol y asignar los mapas correspondientes
        switch (role) {
            case SUBJECT:
                numEntries = Math.toIntExact(dictionary.getNsubjects());
                map = subjectMap;
                reverseMap = reverseSubjectMap;
//                logger.info("Cargando SUBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                System.out.println("Cargando SUBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            case PREDICATE:
                numEntries = Math.toIntExact(dictionary.getNpredicates());
                map = predicateMap;
                reverseMap = reversePredicateMap;
//                logger.info("Cargando PREDICATEs (SERIALIZABLEDICTIONARY): " + numEntries);
                System.out.println("Cargando PREDICATEs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            case OBJECT:
                numEntries = Math.toIntExact(dictionary.getNobjects());
                map = objectMap;
                reverseMap = reverseObjectMap;
//                logger.info("Cargando OBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                System.out.println("Cargando OBJECTs (SERIALIZABLEDICTIONARY): " + numEntries);
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }

        // Procesar cada entrada de la sección
        for (int i = 1; i <= numEntries; i++) {
            Object result = dictionary.idToString(i, role);  // Obtener el valor del diccionario

            // Convierte DelayedString a String usando toString()
            String uri = (result != null) ? result.toString() : null;

            if (uri != null && !uri.isEmpty()) {
                map.put(uri, i);
                reverseMap.put(i, uri);
//                logger.info(String.format("ID=%d, Role=%s, URI=%s", i, role, uri));  // Log de cada URI y su rol
                System.out.println(String.format("ID=%d, Role=%s, URI=%s", i, role, uri));  // Log de cada URI y su rol
            } else {
//                logger.warning(String.format("URI vacía o nula para el ID=%d, Role=%s", i, role));
                System.out.println(String.format("URI vacía o nula para el ID=%d, Role=%s", i, role));
            }
        }
    }

    // Metodo para convertir una URI a su ID correspondiente según el rol (sujeto, predicado, objeto)
    public Integer stringToID(String value, TripleComponentRole role) {
        Integer id = null;
        switch (role) {
            case SUBJECT:
                id = subjectMap.get(value);
                break;
            case PREDICATE:
                id = predicateMap.get(value);
                System.out.println("Verificando predicado en el mapa (SERIALIZABLEDICTIONARY STRINGTOID): " + value);
                if (!predicateMap.containsKey(value)) {
                    System.out.println("Error: El predicado no se encuentra en el mapa (SERIALIZABLEDICTIONARY STRINGTOID): " + value);
                } else {
                    System.out.println("Predicado encontrado en el mapa (SERIALIZABLEDICTIONARY STRINGTOID): " + value + " -> ID: " + id);
                }
                break;
            case OBJECT:
                id = objectMap.get(value);
                break;
        }

        if (id == null) {
            System.out.println("Error: No se encontró el ID para value: " + value + ", role: " + role + " (SERIALIZABLEDICTIONARY STRINGTOID)");
            return -1;  // Para manejar el caso donde el ID no existe
        }
        return id;
    }


    // Metodo para convertir un ID a su URI correspondiente según el rol (sujeto, predicado, objeto)
    public String idToString(int id, TripleComponentRole role) {
        String value = null;
        switch (role) {
            case SUBJECT:
                value = reverseSubjectMap.get(id);
                break;
            case PREDICATE:
                value = reversePredicateMap.get(id);
//                System.out.println("Asignando ID para predicado URI (SERIALIZABLEDICTIONARY): " + value + " -> ID: " + id);
                System.out.println("Asignando ID para predicado URI (SERIALIZABLEDICTIONARY): " + value + " -> ID: " + id);
                break;
            case OBJECT:
                value = reverseObjectMap.get(id);
                break;
        }

        if (value == null) {
//            logger.severe("idToString: No value found for id (SERIALIZABLEDICTIONARY): " + id + ", role: " + role);
            System.out.println("idToString: No value found for id (SERIALIZABLEDICTIONARY): " + id + ", role: " + role);
            return null;
        }
//        logger.info(String.format("idToString: id=%d, role=%s, value=%s (SERIALIZABLEDICTIONARY)", id, role, value));
        System.out.println(String.format("idToString: id=%d, role=%s, value=%s (SERIALIZABLEDICTIONARY)", id, role, value));
        return value;
    }

    @Override
    public String toString() {
        return "SerializableDictionary{...}";  // No se imprime todo el contenido para evitar grandes volúmenes de datos en el log
    }
}
