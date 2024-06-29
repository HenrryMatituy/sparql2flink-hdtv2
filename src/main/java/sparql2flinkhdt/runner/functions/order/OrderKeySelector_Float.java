package sparql2flinkhdt.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Float implements KeySelector<SolutionMappingHDT, Float> {

	static SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Float(SerializableDictionary dictionary, String k) {
		this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Float getKey(SolutionMappingHDT sm) {
		return Float.parseFloat(TripleIDConvert.idToStringFilter(dictionary, sm.getMapping().get(key)).getLiteralValue().toString());
	}
}
