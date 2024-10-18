package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;

public class Triple2SolutionMapping implements MapFunction<TripleID, SolutionMappingHDT> {

    private String var_s, var_p, var_o;
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
        try {
            if(var_s != null && var_p == null && var_o == null) {
                sm.putMapping(var_s, new Long[]{t.getSubject(), 1L});
            } else if(var_s != null && var_p != null && var_o == null) {
                sm.putMapping(var_s, new Long[]{t.getSubject(), 1L});
                sm.putMapping(var_p, new Long[]{t.getPredicate(), 2L});
            } else if(var_s != null && var_p == null && var_o != null) {
                sm.putMapping(var_s, new Long[]{t.getSubject(), 1L});
                sm.putMapping(var_o, new Long[]{t.getObject(), 3L});
            } else if(var_s == null && var_p != null && var_o == null) {
                sm.putMapping(var_p, new Long[]{t.getPredicate(), 2L});
            } else if(var_s == null && var_p != null && var_o != null) {
                sm.putMapping(var_p, new Long[]{t.getPredicate(), 2L});
                sm.putMapping(var_o, new Long[]{t.getObject(), 3L});
            } else if(var_s == null && var_p == null && var_o != null) {
                sm.putMapping(var_o, new Long[]{t.getObject(), 3L});
            } else {
                if (var_s != null) {
                    sm.putMapping(var_s, new Long[]{t.getSubject(), 1L});
                }
                if (var_p != null) {
                    sm.putMapping(var_p, new Long[]{t.getPredicate(), 2L});
                }
                if (var_o != null) {
                    sm.putMapping(var_o, new Long[]{t.getObject(), 3L});
                }
            }
        } catch (Exception e) {
            System.err.println("Excepción en el método map: " + e.getMessage());
            e.printStackTrace();
            throw e; // Re-lanzar la excepción para que Flink la maneje adecuadamente
        }
        return sm;
    }


}
