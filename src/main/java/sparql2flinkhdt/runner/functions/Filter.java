package sparql2flinkhdt.runner.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import sparql2flinkhdt.runner.SerializableDictionary;

// SolutionMapping to SolutionMapping - Filter Function
public class Filter implements FilterFunction<SolutionMappingHDT> {

    private SerializableDictionary dictionary;
    private String expression;

    public Filter(SerializableDictionary dictionary, String expression) {
        this.dictionary = dictionary;
        this.expression = expression;
    }

    @Override
    public boolean filter(SolutionMappingHDT sm) {
        return sm.filter(dictionary, expression);
    }
}
