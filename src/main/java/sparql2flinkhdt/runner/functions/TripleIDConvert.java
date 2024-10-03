package sparql2flinkhdt.runner.functions;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.util.logging.Logger;

public class TripleIDConvert {

    private static final Logger logger = Logger.getLogger(TripleIDConvert.class.getName());

    // Método para convertir un ID en un Node de Jena (URI o literal)
    public static Node idToString(SerializableDictionary dictionary, Integer[] id) {
        if (id == null || id.length != 2) {
            logger.severe("idToString: Invalid ID array: " + id);
            return null;
        }

        String uri = dictionary.idToString(id[0], getRole(id[1]));
        if (uri == null) {
            logger.severe("idToString: URI is null for id: " + id[0] + ", role: " + getRole(id[1]));
            return null;
        }

        // Si es un literal (contiene comillas dobles)
        if (id[1] == 3 && uri.startsWith("\"")) {
            return createLiteralNode(uri);  // Crear un nodo literal
        }

        // Si no es un literal, crear un nodo URI
        return NodeFactory.createURI(uri);
    }

    // Método para convertir una cadena (URI o literal) a un ID basado en el rol (sujeto, predicado, objeto)
    public static Integer stringToID(SerializableDictionary dictionary, String element, TripleComponentRole role) {
        Integer id = dictionary.stringToID(element, role);
        if (id == null || id == -1) {
            logger.severe("stringToID: No ID found for element: " + element + " with role: " + role);
            return null;
        }
        return id;
    }

    // Método auxiliar para convertir un ID en un nodo (Node) de Jena
    public static String idToString(SerializableDictionary dictionary, int id, TripleComponentRole role) {
        return dictionary.idToString(id, role);
    }

    // Método auxiliar para convertir un literal con tipo XSD en un nodo
    private static Node createLiteralNode(String object) {
        int start = object.indexOf("\"") + 1;
        int end = object.lastIndexOf("\"");
        if (start < 0 || end < 0 || start >= end) {
            logger.severe("createLiteralNode: Invalid literal format: " + object);
            return null;
        }

        String value = object.substring(start, end);

        // Si el literal tiene un tipo (e.g., "value"^^<type>)
        if (object.contains("^^")) {
            String type = object.substring(object.indexOf("^^") + 2);
            return NodeFactory.createLiteral(value, getXSDDatatype(type));
        }

        // Si no tiene tipo, devolver un literal simple
        return NodeFactory.createLiteral(value);
    }

    // Método auxiliar para devolver el tipo de XSD correcto
    private static XSDDatatype getXSDDatatype(String datatypeURI) {
        switch (datatypeURI) {
            case "http://www.w3.org/2001/XMLSchema#integer":
                return XSDDatatype.XSDinteger;
            case "http://www.w3.org/2001/XMLSchema#boolean":
                return XSDDatatype.XSDboolean;
            case "http://www.w3.org/2001/XMLSchema#dateTime":
                return XSDDatatype.XSDdateTime;
            case "http://www.w3.org/2001/XMLSchema#string":
                return XSDDatatype.XSDstring;
            // Otros tipos de XSD pueden agregarse aquí
            default:
                logger.warning("getXSDDatatype: Unknown datatype URI: " + datatypeURI);
                return XSDDatatype.XSDstring;  // Valor por defecto
        }
    }

    // Método para obtener el rol a partir del código
    private static TripleComponentRole getRole(Integer roleCode) {
        if (roleCode == null) {
            logger.severe("getRole: Null role code");
            return null;
        }

        switch (roleCode) {
            case 1:
                return TripleComponentRole.SUBJECT;
            case 2:
                return TripleComponentRole.PREDICATE;
            case 3:
                return TripleComponentRole.OBJECT;
            default:
                throw new IllegalArgumentException("Unknown role code: " + roleCode);
        }
    }

    // Método idToStringFilter para convertir IDs a Node con manejo de literales
    public static Node idToStringFilter(SerializableDictionary dictionary, Integer[] id) {
        if (id == null || id.length != 2) {
            logger.severe("idToStringFilter: Invalid ID array: " + id);
            return null;
        }

        String uri = dictionary.idToString(id[0], getRole(id[1]));

        if (uri == null) {
            logger.severe("idToStringFilter: URI is null for id: " + id[0] + ", role: " + getRole(id[1]));
            return null;
        }

        // Si es un literal, creamos un nodo literal
        if (id[1] == 3 && uri.startsWith("\"")) {
            return createLiteralNode(uri);
        }

        // Si no es un literal, devolvemos el nodo URI
        return NodeFactory.createURI(uri);
    }
}
