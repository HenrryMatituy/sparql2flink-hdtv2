package sparql2flinkhdt.runner.functions;

import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.sse.SSE;
import sparql2flinkhdt.runner.SerializableDictionary;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SolutionMappingHDT implements Serializable {

    private static final long serialVersionUID = 1L;
    private HashMap<String, Integer[]> mapping = new HashMap<>();
    private SerializableDictionary serializableDictionary;

    private static final Logger logger = Logger.getLogger(SolutionMappingHDT.class.getName());

    public SolutionMappingHDT() {}

    public SolutionMappingHDT(HashMap<String, Integer[]> sm) {
        this.mapping = sm;
    }

    public SolutionMappingHDT(SerializableDictionary serializableDictionary) {
        this.serializableDictionary = serializableDictionary;
    }

    public void setMapping(HashMap<String, Integer[]> mapping) {
        this.mapping = mapping;
    }

    public HashMap<String, Integer[]> getMapping() {
        return mapping;
    }

    public void putMapping(String var, Integer[] val) {
        mapping.put(var, val);
        logger.info(String.format("putMapping: var=%s, val=%s", var, val));
    }

    public Integer[] getValue(String var) {
        return mapping.get(var);
    }

    public boolean existMapping(String var, Integer val) {
        boolean flag = mapping.containsKey(var) && mapping.get(var)[0].equals(val);
        logger.info(String.format("existMapping: var=%s, val=%d, exists=%b", var, val, flag));
        return flag;
    }

    public SolutionMappingHDT join(SolutionMappingHDT sm) {
        for (Map.Entry<String, Integer[]> hm : sm.getMapping().entrySet()) {
            if (!existMapping(hm.getKey(), hm.getValue()[0])) {
                this.putMapping(hm.getKey(), hm.getValue());
            }
        }
        return this;
    }

    public SolutionMappingHDT leftJoin(SolutionMappingHDT sm) {
        if (sm != null) {
            for (Map.Entry<String, Integer[]> hm : sm.getMapping().entrySet()) {
                if (!existMapping(hm.getKey(), hm.getValue()[0])) {
                    this.putMapping(hm.getKey(), hm.getValue());
                }
            }
        }
        return this;
    }

    public SolutionMappingHDT newSolutionMapping(String[] vars) {
        SolutionMappingHDT sm = new SolutionMappingHDT();
        for (String var : vars) {
            if (var != null) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
    }

    public SolutionMappingHDT project(String[] vars) {
        SolutionMappingHDT sm = new SolutionMappingHDT();
        for (String var : vars) {
            if (mapping.containsKey(var)) {
                sm.putMapping(var, mapping.get(var));
            }
        }
        return sm;
    }

    public boolean filter(SerializableDictionary dictionary, String expression) {
        Expr expr = SSE.parseExpr(expression);
        boolean result = FilterConvert.convert(dictionary, expr, this.mapping);
        logger.info(String.format("filter: expression=%s, result=%b", expression, result));
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sm = new StringBuilder();
        for (Map.Entry<String, Integer[]> hm : mapping.entrySet()) {
            if (hm.getValue() != null) {
                sm.append(hm.getKey()).append("-->").append(hm.getValue()[0]).append("\t");
            }
        }
        return sm.toString();
    }

    // Métodos para convertir a y desde SerializableDictionary
    public SerializableDictionary toSerializableDictionary() {
        return serializableDictionary;
    }

    public static SolutionMappingHDT fromSerializableDictionary(SerializableDictionary serializableDictionary) {
        SolutionMappingHDT solutionMappingHDT = new SolutionMappingHDT(serializableDictionary);
        // Aquí puedes agregar lógica adicional si es necesario para inicializar mapping desde serializableDictionary
        return solutionMappingHDT;
    }
}
