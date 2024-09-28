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
        try {
            System.out.println("Cargando SUBJECTs...");
            long maxSubjects = dictionary.getNsubjects();  // Obtener el número de sujetos
            for (int id = 1; id <= maxSubjects; id++) {
                Object subjectObj = dictionary.idToString(id, TripleComponentRole.SUBJECT);  // Aquí puede ser un DelayedString
                String subject = subjectObj != null ? subjectObj.toString() : null;  // Convertir a String explícitamente
                if (subject != null) {
                    System.out.println("Sujeto ID: " + id + " URI: " + subject);
                    subjectMap.put(subject, id);
                    reverseSubjectMap.put(id, subject);
                } else {
                    System.out.println("Error: Sujeto con ID " + id + " no encontrado.");
                }
            }

            System.out.println("Cargando PREDICATEs...");
            long maxPredicates = dictionary.getNpredicates();  // Obtener el número de predicados
            for (int id = 1; id <= maxPredicates; id++) {
                Object predicateObj = dictionary.idToString(id, TripleComponentRole.PREDICATE);  // Puede ser DelayedString
                String predicate = predicateObj != null ? predicateObj.toString() : null;  // Convertir a String explícitamente
                if (predicate != null) {
                    System.out.println("Predicado ID: " + id + " URI: " + predicate);
                    predicateMap.put(predicate, id);
                    reversePredicateMap.put(id, predicate);
                } else {
                    System.out.println("Error: Predicado con ID " + id + " no encontrado.");
                }
            }

            System.out.println("Cargando OBJECTs...");
            long maxObjects = dictionary.getNobjects();  // Obtener el número de objetos
            for (int id = 1; id <= maxObjects; id++) {
                Object objectObj = dictionary.idToString(id, TripleComponentRole.OBJECT);  // Puede ser DelayedString
                String object = objectObj != null ? objectObj.toString() : null;  // Convertir a String explícitamente
                if (object != null) {
                    System.out.println("Objeto ID: " + id + " URI: " + object);
                    objectMap.put(object, id);
                    reverseObjectMap.put(id, object);
                } else {
                    System.out.println("Error: Objeto con ID " + id + " no encontrado.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//    public void loadFromHDTDictionary(Dictionary dictionary) {
//        loadSection(dictionary, TripleComponentRole.SUBJECT);
//        loadSection(dictionary, TripleComponentRole.PREDICATE);
//        loadSection(dictionary, TripleComponentRole.OBJECT);
//    }

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
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }

        // Procesar cada entrada de la sección
        for (int i = 1; i <= numEntries; i++) {

            Object result = dictionary.idToString(i, role);  // Obtener el valor del diccionario
            String uri = result != null ? result.toString() : null;

            if (uri != null && !uri.isEmpty()) {
                map.put(uri, i);
                reverseMap.put(i, uri);
                logger.info(String.format("IDDDDD=%d, Role=%s, URI=%s", i, role, uri));  // Log de cada URI y su rol
            } else {
                logger.warning(String.format("URI vacía o nula para el ID=%d, Role=%s", i, role));
            }
        }
    }



//    public int stringToID(String value, TripleComponentRole role) {
//        Integer id;
//        switch (role) {
//            case SUBJECT:
//                id = subjectMap.get(value);
//                logger.info(String.format("stringToIDDD Sujeto: value=%s, role=%s, id=%d", value, role, id));
//                break;
//            case PREDICATE:
//                id = predicateMap.get(value);
//                logger.info(String.format("stringToIDDD Predicado: value=%s, role=%s, id=%d", value, role, id));
//                break;
//            case OBJECT:
//                id = objectMap.get(value);
//                logger.info(String.format("stringToIDDD Objeto: value=%s, role=%s, id=%d", value, role, id));
//                break;
//            default:
//                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
//
//        }
//        if (id == null) {
//            logger.severe("stringToID: No ID found for value: " + value + ", role: " + role);
//            return -1;
//        }
//
//        // Prueba forzada
////        logger.info("Prueba forzada con SUBJECT");
////        stringToID("http://example.org/alice", TripleComponentRole.SUBJECT);
////
////        logger.info("Prueba forzada con OBJECT");
////        stringToID("http://example.org/alice", TripleComponentRole.OBJECT);
//      return id;
//
//    }

    public int stringToID(String value, TripleComponentRole role) {
        Integer id = null;
        switch (role) {
            case SUBJECT:
                id = subjectMap.get(value);
                logger.info(String.format("stringToID Sujeto desde SerializableDictionary: value=%s, role=%s, id=%d",
                        value, role, id));
                break;
            case PREDICATE:
                id = predicateMap.get(value);
                logger.info(String.format("stringToID Predicado esde SerializableDictionary: value=%s, role=%s, id=%d", value, role, id));
                break;
            case OBJECT:
                id = objectMap.get(value);
                logger.info(String.format("stringToID Objeto esde SerializableDictionary: value=%s, role=%s, id=%d", value, role, id));
                break;
            default:
                throw new IllegalArgumentException("Unknown TripleComponentRole: " + role);
        }
        if (id == null) {
            logger.severe("stringToID: No ID found for value: " + value + ", role: " + role);
            return -1;
        }
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

//    private void loadSection(Dictionary dictionary, TripleComponentRole role) {
//        int numEntries = 0;
//        switch (role) {
//            case SUBJECT:
//                numEntries = Math.toIntExact(dictionary.getNsubjects());
//                break;
//            case PREDICATE:
//                numEntries = Math.toIntExact(dictionary.getNpredicates());
//                break;
//            case OBJECT:
//                numEntries = Math.toIntExact(dictionary.getNobjects());
//                break;
//        }
//
////        for (int i = 1; i <= numEntries; i++) {
////            String uri = null;
////            switch (role) {
////                case SUBJECT:
////                    uri = (String) dictionary.idToString(i, role);
////                    subjectMap.put(uri, i);
////                    reverseSubjectMap.put(i, uri);
////                    break;
////                case PREDICATE:
////                    uri = String.valueOf(dictionary.idToString(i, TripleComponentRole.PREDICATE));
////                    predicateMap.put(uri, i);
////                    reversePredicateMap.put(i, uri);
////                    break;
////                case OBJECT:
////                    uri = String.valueOf(dictionary.idToString(i, TripleComponentRole.OBJECT));
////                    objectMap.put(uri, i);
////                    reverseObjectMap.put(i, uri);
////                    break;
////            }
////        }
//
//        for (int i = 1; i <= numEntries; i++) {
//            String uri = dictionary.idToString(i, role);  // Sin String.valueOf
//            if (uri != null && !uri.isEmpty()) {
//                map.put(uri, i);
//                reverseMap.put(i, uri);
//            } else {
//                logger.warning("Empty or null URI for role: " + role + " and ID: " + i);
//            }
//        }
//    }
