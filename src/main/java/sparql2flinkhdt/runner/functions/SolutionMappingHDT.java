package sparql2flinkhdt.runner.functions;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.sse.SSE;
import sparql2flinkhdt.runner.SerializableDictionary;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SolutionMappingHDT implements Serializable {
    private SerializableDictionary serializableDictionary;
    private static final Logger logger = Logger.getLogger(SolutionMappingHDT.class.getName());
    private HashMap<String, Integer[]> mapping = new HashMap<>();

    public SolutionMappingHDT() {}

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
    }

    public boolean existMapping(String var, Integer val) {
        return mapping.containsKey(var) && mapping.get(var)[0].equals(val);
    }

    public SolutionMappingHDT join(SolutionMappingHDT sm) {
        for (Map.Entry<String, Integer[]> hm : sm.getMapping().entrySet()) {
            if (!existMapping(hm.getKey(), hm.getValue()[0])) {
                this.putMapping(hm.getKey(), hm.getValue());
            }
        }
        return this;
    }

    public SolutionMappingHDT leftJoin(SolutionMappingHDT right) {
        SolutionMappingHDT result = new SolutionMappingHDT();

        // Recorremos todas las entradas del mapping actual (izquierdo)
        for (Map.Entry<String, Integer[]> leftEntry : this.mapping.entrySet()) {
            String var = leftEntry.getKey();
            Integer[] leftValue = leftEntry.getValue();

            // Si existe un mapeo correspondiente en el derecho, usamos su valor
            if (right != null && right.getMapping().containsKey(var)) {
                Integer[] rightValue = right.getMapping().get(var);
                result.putMapping(var, rightValue != null ? rightValue : leftValue);
            } else {
                result.putMapping(var, leftValue);
            }
        }

        // Añadir cualquier clave del lado derecho que no esté en el izquierdo
        if (right != null) {
            for (Map.Entry<String, Integer[]> rightEntry : right.getMapping().entrySet()) {
                if (!result.getMapping().containsKey(rightEntry.getKey())) {
                    result.putMapping(rightEntry.getKey(), rightEntry.getValue());
                }
            }
        }

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

    // Método para aplicar un filtro basado en una expresión SPARQL
    public boolean filter(SerializableDictionary dictionary, String expression) {
        try {
            Expr expr = SSE.parseExpr(expression);  // Parseamos la expresión
            for (Map.Entry<String, Integer[]> entry : mapping.entrySet()) {
                Integer[] value = entry.getValue();
                Node node = TripleIDConvert.idToString(dictionary, value);  // Convertimos a Node
                NodeValue nodeValue = NodeValue.makeNode(node);

                // Evaluamos la expresión sobre los datos
                NodeValue result = expr.eval((Binding) nodeValue, null);
                if (!result.getBoolean()) {
                    return false;  // Si algún resultado no pasa el filtro, retornamos false
                }
            }
            return true;  // Si todos pasan el filtro, retornamos true
        } catch (ExprEvalException e) {
            logger.severe("Error al evaluar la expresión: " + e.getMessage());
            return false;
        }
    }

    // Crear una nueva instancia de SolutionMapping solo con las variables especificadas
    public SolutionMappingHDT newSolutionMapping(String[] vars) {
        SolutionMappingHDT newMapping = new SolutionMappingHDT();
        for (String var : vars) {
            if (this.mapping.containsKey(var)) {
                newMapping.putMapping(var, this.mapping.get(var));  // Copiar la variable y su valor
            }
        }
        return newMapping;
    }

    // Proyectar solo las variables especificadas en una nueva instancia de SolutionMapping
    public SolutionMappingHDT project(String[] vars) {
        SolutionMappingHDT projectedMapping = new SolutionMappingHDT(this.serializableDictionary);
        for (String var : vars) {
            if (this.mapping.containsKey(var)) {
                projectedMapping.putMapping(var, this.mapping.get(var));
            }
        }
        return projectedMapping;
    }
}
