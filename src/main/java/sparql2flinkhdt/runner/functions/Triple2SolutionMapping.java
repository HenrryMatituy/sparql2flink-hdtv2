package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

public class Triple2SolutionMapping implements MapFunction<TripleID, SolutionMappingHDT> {

    private String var_s, var_p, var_o = null;
    private SerializableDictionary serializableDictionary;

    public Triple2SolutionMapping(String s, String p, String o, SerializableDictionary serializableDictionary){
        this.var_s = s;
        this.var_p = p;
        this.var_o = o;
        this.serializableDictionary = serializableDictionary;
    }

    @Override
    public SolutionMappingHDT map(TripleID t) {
        SolutionMappingHDT sm = new SolutionMappingHDT(serializableDictionary);
        if(var_s != null && var_p == null && var_o == null) {
            sm.putMapping(var_s, new Integer[]{Math.toIntExact(t.getSubject()), 1});
        } else if(var_s != null && var_p != null && var_o == null) {
            sm.putMapping(var_s, new Integer[]{Math.toIntExact(t.getSubject()), 1});
            sm.putMapping(var_p, new Integer[]{Math.toIntExact(t.getPredicate()), 2});
        } else if(var_s != null && var_p == null && var_o != null) {
            sm.putMapping(var_s, new Integer[]{Math.toIntExact(t.getSubject()), 1});
            sm.putMapping(var_o, new Integer[]{Math.toIntExact(t.getObject()), 3});
        } else if(var_s == null && var_p != null && var_o == null) {
            sm.putMapping(var_p, new Integer[]{Math.toIntExact(t.getPredicate()), 2});
        } else if(var_s == null && var_p != null && var_o != null) {
            sm.putMapping(var_p, new Integer[]{Math.toIntExact(t.getPredicate()), 2});
            sm.putMapping(var_o, new Integer[]{Math.toIntExact(t.getObject()), 3});
        } else if(var_s == null && var_p == null && var_o != null) {
            sm.putMapping(var_o, new Integer[]{Math.toIntExact(t.getObject()), 3});
        } else {
            sm.putMapping(var_s, new Integer[]{Math.toIntExact(t.getSubject()), 1});
            sm.putMapping(var_p, new Integer[]{Math.toIntExact(t.getPredicate()), 2});
            sm.putMapping(var_o, new Integer[]{Math.toIntExact(t.getObject()), 3});
        }
        return sm;
    }
}
