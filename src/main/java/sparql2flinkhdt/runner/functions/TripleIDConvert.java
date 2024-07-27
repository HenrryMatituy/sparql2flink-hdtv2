package sparql2flinkhdt.runner.functions;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.util.logging.Logger;

public class TripleIDConvert {

    private static final Logger logger = Logger.getLogger(TripleIDConvert.class.getName());

    public static Node idToString(SerializableDictionary dictionary, Integer[] id) {
        String uri = dictionary.idToString(id[0], getRole(id[1]));
        if (uri == null) {
            logger.severe("idToString: URI is null for id: " + id[0] + ", role: " + getRole(id[1]));
            return null;
        }
        return NodeFactory.createURI(uri);
    }

    public static Node idToStringFilter(SerializableDictionary dictionary, Integer[] id) {
        TripleComponentRole role = getRole(id[1]);
        String uri = dictionary.idToString(id[0], role);
        if (uri == null) {
            logger.severe("idToStringFilter: URI is null for id: " + id[0] + ", role: " + role);
            return null;
        }

        if (role == TripleComponentRole.OBJECT && uri.contains("^^")) {
            return createLiteralNode(uri);
        } else {
            return NodeFactory.createURI(uri);
        }
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
            case 1: return TripleComponentRole.SUBJECT;
            case 2: return TripleComponentRole.PREDICATE;
            case 3: return TripleComponentRole.OBJECT;
            default: throw new IllegalArgumentException("Unknown role code: " + roleCode);
        }
    }

    private static Node createLiteralNode(String object) {
        int start = 1, end = object.indexOf("^") - 1, hashtag = object.indexOf("#") + 1;
        String literalValue = object.substring(start, end);
        String dataType = object.substring(hashtag, object.length() - 1);

        switch (dataType) {
            case "integer":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDint);
            case "boolean":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDboolean);
            case "dateTime":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDdateTime);
            case "date":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDdate);
            case "decimal":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDdecimal);
            case "double":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDdouble);
            case "byte":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDbyte);
            case "float":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDfloat);
            case "long":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDlong);
            case "string":
                return NodeFactory.createLiteral(literalValue, XSDDatatype.XSDstring);
            default:
                throw new IllegalArgumentException("Unknown data type: " + dataType);
        }
    }
}
