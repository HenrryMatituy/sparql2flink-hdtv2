package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Node;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

// SolutionMappingHDT to SolutionMappingURI - Map Function
public class TripleID2TripleString implements MapFunction<SolutionMappingHDT, SolutionMappingURI> {

    private SerializableDictionary dictionary;

    public TripleID2TripleString(SerializableDictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public SolutionMappingURI map(SolutionMappingHDT sm) {
        SolutionMappingURI smURI = new SolutionMappingURI();
        for (String var : sm.getMapping().keySet()) {
            Integer[] id = sm.getMapping().get(var);
            Node node = TripleIDConvert.idToString(dictionary, id);
            smURI.putMapping(var, node);
        }
        return smURI;
    }
}
