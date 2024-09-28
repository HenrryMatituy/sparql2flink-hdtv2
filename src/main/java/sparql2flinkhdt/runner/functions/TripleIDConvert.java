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
            logger.severe("idToString: Invalid ID array: " + Arrays.toString(id));
            return null;
        }

        // Convertir el ID en una URI o literal
        String uri = dictionary.idToString(id[0], getRole(id[1]));
        if (uri == null) {
            logger.severe("idToString: URI is null for id: " + id[0] + ", role: " + getRole(id[1]));
            return null;
        }

        // Si el componente es un literal (objeto), devolver un nodo literal
        if (getRole(id[1]) == TripleComponentRole.OBJECT && uri.startsWith("\"")) {
            return createLiteralNode(uri);  // Crear un nodo literal
        } else {
            return NodeFactory.createURI(uri);  // Crear un nodo URI
        }
    }

    // Método helper para convertir literales con tipos de datos
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

    // Helper para obtener el rol (subject, predicate, object) según el código
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
}