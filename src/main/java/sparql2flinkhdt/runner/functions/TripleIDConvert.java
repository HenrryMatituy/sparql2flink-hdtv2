package sparql2flinkhdt.runner.functions;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.util.Arrays;
import java.util.logging.Logger;

public class TripleIDConvert {

    private static final Logger logger = Logger.getLogger(TripleIDConvert.class.getName());

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
        return NodeFactory.createURI(uri);
    }

    public static Integer stringToIDSubject(SerializableDictionary dictionary, String element) {
        return dictionary.stringToID(element, TripleComponentRole.SUBJECT);
    }

    public static Integer stringToIDPredicate(SerializableDictionary dictionary, String element) {
        return dictionary.stringToID(element, TripleComponentRole.PREDICATE);
    }

    public static Integer stringToIDObject(SerializableDictionary dictionary, String element) {
        return dictionary.stringToID(element, TripleComponentRole.OBJECT);
    }

    private static TripleComponentRole getRole(int roleCode) {
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

    public static Node idToStringFilter(SerializableDictionary dictionary, Integer[] id) {
        if (id == null || id.length != 2) {
            logger.severe("idToStringFilter: Invalid ID array: " + Arrays.toString(id));
            return null;
        }

        TripleComponentRole role = getRole(id[1]);
        String uri = dictionary.idToString(id[0], role);

        if (uri == null) {
            logger.severe("idToStringFilter: URI is null for id: " + id[0] + ", role: " + role);
            return null;
        }

        // Si es un literal con un tipo de dato, intentamos crear un nodo literal con el tipo XSD adecuado
        if (role == TripleComponentRole.OBJECT && uri.contains("^^")) {
            return createLiteralNode(uri);  // Creación de nodos literales con tipos de datos XSD
        } else {
            // Si no es un literal, devolvemos el nodo URI normal
            return NodeFactory.createURI(uri);
        }
    }

    private static Node createLiteralNode(String object) {
        int start = object.indexOf("\"") + 1;
        int end = object.indexOf("\"^^");
        int hashtag = object.indexOf("#") + 1;

        String value = object.substring(start, end);
        String type = object.substring(hashtag, object.length() - 1);

        switch (type) {
            case "integer":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDint);
            case "boolean":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDboolean);
            case "dateTime":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDdateTime);
            case "date":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDdate);
            case "decimal":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDdecimal);
            case "double":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDdouble);
            case "byte":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDbyte);
            case "float":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDfloat);
            case "long":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDlong);
            case "string":
                return NodeFactory.createLiteral(value, XSDDatatype.XSDstring);
            default:
                return NodeFactory.createLiteral(value);  // Caso por defecto sin tipo explícito
        }
    }


}
