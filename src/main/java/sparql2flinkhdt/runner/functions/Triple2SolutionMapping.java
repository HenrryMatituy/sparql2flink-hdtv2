package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT.MappingValue;

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
                sm.putMapping(var_s, new MappingValue(t.getSubject(), 1));
            } else if(var_s != null && var_p != null && var_o == null) {
                sm.putMapping(var_s, new MappingValue(t.getSubject(), 1));
                sm.putMapping(var_p, new MappingValue(t.getPredicate(), 2));
            } else if(var_s != null && var_p == null && var_o != null) {
                sm.putMapping(var_s, new MappingValue(t.getSubject(), 1));
                sm.putMapping(var_o, new MappingValue(t.getObject(), 3));
            } else if(var_s == null && var_p != null && var_o == null) {
                sm.putMapping(var_p, new MappingValue(t.getPredicate(), 2));
            } else if(var_s == null && var_p != null && var_o != null) {
                sm.putMapping(var_p, new MappingValue(t.getPredicate(), 2));
                sm.putMapping(var_o, new MappingValue(t.getObject(), 3));
            } else if(var_s == null && var_p == null && var_o != null) {
                sm.putMapping(var_o, new MappingValue(t.getObject(), 3));
            } else {
                if (var_s != null) {
                    sm.putMapping(var_s, new MappingValue(t.getSubject(), 1));
                }
                if (var_p != null) {
                    sm.putMapping(var_p, new MappingValue(t.getPredicate(), 2));
                }
                if (var_o != null) {
                    sm.putMapping(var_o, new MappingValue(t.getObject(), 3));
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
