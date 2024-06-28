package sparql2flinkhdt.runner;

import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;

import java.io.Serializable;

public class SerializableDictionary implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Dictionary dictionary;

    public SerializableDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    public int stringToID(String value, TripleComponentRole role) {
        return (int) dictionary.stringToId(value, role);
    }

    public String idToString(int id, TripleComponentRole role) {
        return (String) dictionary.idToString(id, role);
    }
}



//
//import org.rdfhdt.hdt.dictionary.Dictionary;
//import org.rdfhdt.hdt.enums.RDFNotation;
//
//import java.io.Serializable;
//
//public class SerializableDictionary implements Serializable {
//    private transient Dictionary dictionary;
//
//    public SerializableDictionary(Dictionary dictionary) {
//        this.dictionary = dictionary;
//    }
//
//    // Methods for conversion
//    public Integer stringToIDSubject(String subject) {
//        return (int) dictionary.stringToId(subject,);
//    }
//
//    public Integer stringToIDPredicate(String predicate) {
//        return (int) dictionary.stringToId(predicate, RDFNodeType.PREDICATE);
//    }
//
//    public Integer stringToIDObject(String object) {
//        return (int) dictionary.stringToId(object, RDFNodeType.OBJECT);
//    }
//
//    // Getters and setters for the dictionary field
//    public Dictionary getDictionary() {
//        return dictionary;
//    }
//
//    public void setDictionary(Dictionary dictionary) {
//        this.dictionary = dictionary;
//    }
//}
