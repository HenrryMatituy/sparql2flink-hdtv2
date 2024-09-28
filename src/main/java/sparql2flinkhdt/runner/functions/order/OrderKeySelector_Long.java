package sparql2flinkhdt.runner.functions.order;

import org.apache.flink.api.java.functions.KeySelector;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.functions.SolutionMappingHDT;
import sparql2flinkhdt.runner.functions.TripleIDConvert;

// SolutionMapping - Key Selector Order by
public class OrderKeySelector_Long implements KeySelector<SolutionMappingHDT, Long> {

	static SerializableDictionary dictionary;
	private String key;

	public OrderKeySelector_Long(SerializableDictionary dictionary, String k) {
		this.dictionary = dictionary;
		this.key = k;
	}

	@Override
	public Long getKey(SolutionMappingHDT sm) {
		// Obtener el ID del mapping
		Integer[] idAndRole = sm.getMapping().get(key); // Supongo que este es el formato que tienes en SolutionMappingHDT

		// Verificar que el ID y rol existan
		if (idAndRole == null || idAndRole.length != 2) {
			throw new IllegalArgumentException("El formato de idAndRole no es válido para la clave: " + key);
		}

		// Usar idToStringFilter para convertir el ID y rol en un valor de cadena y luego obtener el valor largo
		String literalValue = TripleIDConvert.idToStringFilter(dictionary, idAndRole).getLiteralValue().toString();

		return Long.parseLong(literalValue);
	}
}
