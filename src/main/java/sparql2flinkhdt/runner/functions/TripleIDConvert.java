package sparql2flinkhdt.runner.functions;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import sparql2flinkhdt.runner.SerializableDictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

public class TripleIDConvert {

    public static Node idToString(SerializableDictionary dictionary, Integer[] id) {
        Node element;
        if (id[1] == 1) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.SUBJECT));
        } else if (id[1] == 2) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.PREDICATE));
        } else {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.OBJECT));
        }
        return element;
    }

    public static Node idToStringFilter(SerializableDictionary dictionary, Integer[] id) {
        Node element = null;
        if (id[1] == 1) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.SUBJECT));
        } else if (id[1] == 2) {
            element = NodeFactory.createURI(dictionary.idToString(id[0], TripleComponentRole.PREDICATE));
        } else {
            String object = dictionary.idToString(id[0], TripleComponentRole.OBJECT);
            if(object.contains("^^")){
                int start = 1, end = object.indexOf("^")-1, hashtag = object.indexOf("#")+1;
                switch (object.substring(hashtag, object.length()-1)) {
                    case "integer":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDint);
                        break;
                    case "boolean":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDboolean);
                        break;
                    case "dateTime":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdateTime);
                        break;
                    case "date":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdate);
                        break;
                    case "decimal":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdecimal);
                        break;
                    case "double":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDdouble);
                        break;
                    case "byte":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDbyte);
                        break;
                    case "float":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDfloat);
                        break;
                    case "long":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDlong);
                        break;
                    case "string":
                        element = NodeFactory.createLiteral(object.substring(start, end), XSDDatatype.XSDstring);
                        break;
                }
            } else {
                element = NodeFactory.createURI(object);
            }
        }
        return element;
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
}
