package sparql2flinkhdt.out;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import sparql2flinkhdt.runner.SerializableDictionary;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.functions.*;

import java.util.ArrayList;
import java.util.Map;

public class Query {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		if (!params.has("dataset") && !params.has("output")) {
			System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));

		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while (iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

		// Creating SerializableDictionary
		SerializableDictionary serializableDictionary = new SerializableDictionary();
		serializableDictionary.loadFromHDTDictionary(hdt.getDictionary());

		// Verificación del contenido del diccionario
//		System.out.println("Verificando el contenido del diccionario:");

		// Asumamos que sabemos que los IDs están en el rango de 1 a 10. Puedes ajustar este rango según tus necesidades.
		for (int id = 1; id <= 10; id++) {
			String subject = serializableDictionary.idToString(id, TripleComponentRole.SUBJECT);
			String predicate = serializableDictionary.idToString(id, TripleComponentRole.PREDICATE);
			String object = serializableDictionary.idToString(id, TripleComponentRole.OBJECT);

			// Mostrar valores si son válidos
			if (subject != null) {
				System.out.println("Sujeto ID: " + id + " URI: " + subject);
			}
			if (predicate != null) {
				System.out.println("Predicado ID: " + id + " URI: " + predicate);
			}
			if (object != null) {
				System.out.println("Objeto ID: " + id + " URI: " + object);
			}
		}


		DataSet<SolutionMappingHDT> sm1 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/name", null))
				.map(new Triple2SolutionMapping("?person", null, "?name", serializableDictionary));

		DataSet<SolutionMappingHDT> sm2 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/mbox", null))
				.map(new Triple2SolutionMapping("?person", null, "?mbox", serializableDictionary));

		DataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)
				.where(new JoinKeySelector(new String[]{"?person"}))
				.equalTo(new JoinKeySelector(new String[]{"?person"}))
				.with(new LeftJoin());

		DataSet<SolutionMappingHDT> sm4 = sm3
				.map(new Project(new String[]{"?person", "?name", "?mbox"}));

		DataSet<SolutionMappingURI> sm5 = sm4
				.map(new TripleID2TripleString(serializableDictionary));

		// Sink
		sm5.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);

		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}
