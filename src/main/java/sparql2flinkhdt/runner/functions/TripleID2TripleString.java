package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.jena.graph.Node;
import sparql2flinkhdt.runner.SerializableDictionary;

import java.util.logging.Logger;

public class TripleID2TripleString implements MapFunction<SolutionMappingHDT, SolutionMappingURI> {

    private static final Logger logger = Logger.getLogger(TripleID2TripleString.class.getName());
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
            if (node == null) {
                logger.severe("map: Node is null for variable: " + var + ", id: " + id);
            }
            smURI.putMapping(var, node);
        }
        return smURI;
    }
}
