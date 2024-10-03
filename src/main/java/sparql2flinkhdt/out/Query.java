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

public class Query {
	public static void main(String[] args) throws Exception {
		// Parsear los parámetros de entrada
		final ParameterTool params = ParameterTool.fromArgs(args);

		// Verificación de los parámetros necesarios
		if (!params.has("dataset") || !params.has("output")) {
			System.out.println("Use --dataset para especificar la ruta del dataset y --output para especificar la ruta de salida.");
			return;
		}

		// Crear el entorno de ejecución de Flink
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Cargar los triples del dataset HDT
		HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));

		// Convertir los triples de HDT a una lista de TripleID
		ArrayList<TripleID> listTripleID = new ArrayList<>();
		IteratorTripleID iterator = hdt.getTriples().searchAll();
		while (iterator.hasNext()) {
			TripleID tripleID = new TripleID(iterator.next());
			listTripleID.add(tripleID);
		}

		// Crear un DataSet a partir de la lista de triples
		DataSet<TripleID> dataset = env.fromCollection(listTripleID);

		// Crear el diccionario serializable
		SerializableDictionary serializableDictionary = new SerializableDictionary();
		serializableDictionary.loadFromHDTDictionary(hdt.getDictionary());

		// Verificación del contenido del diccionario
		System.out.println("Verificando el contenido del diccionario:");
		for (TripleID t : listTripleID) {
			// Convertir los IDs de sujeto, predicado y objeto a sus URIs usando el diccionario
			String subject = serializableDictionary.idToString((int) t.getSubject(), TripleComponentRole.SUBJECT);
			String predicate = serializableDictionary.idToString((int) t.getPredicate(), TripleComponentRole.PREDICATE);
			String object = serializableDictionary.idToString((int) t.getObject(), TripleComponentRole.OBJECT);

			// Imprimir el triple resultante
			System.out.println("Triple: " + subject + " " + predicate + " " + object);
		}

		// Crear el primer conjunto de mapeo de soluciones (sm1) para personas y nombres
		DataSet<SolutionMappingHDT> sm1 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/name", null))
				.map(new Triple2SolutionMapping("?person", null, "?name", serializableDictionary));

		// Crear el segundo conjunto de mapeo de soluciones (sm2) para personas y correos electrónicos
		DataSet<SolutionMappingHDT> sm2 = dataset
				.filter(new Triple2Triple(serializableDictionary, null, "http://xmlns.com/foaf/0.1/mbox", null))
				.map(new Triple2SolutionMapping("?person", null, "?mbox", serializableDictionary));

		// Realizar un Left Outer Join entre sm1 y sm2
		DataSet<SolutionMappingHDT> sm3 = sm1.leftOuterJoin(sm2)
				.where(new JoinKeySelector(new String[]{"?person"}))
				.equalTo(new JoinKeySelector(new String[]{"?person"}))
				.with(new LeftJoin());

		// Proyectar las variables ?person, ?name, y ?mbox
		DataSet<SolutionMappingHDT> sm4 = sm3
				.map(new Project(new String[]{"?person", "?name", "?mbox"}));

		// Convertir los IDs a URIs usando el diccionario
		DataSet<SolutionMappingURI> sm5 = sm4
				.map(new TripleID2TripleString(serializableDictionary));

		// Escribir el resultado a un archivo de texto
		sm5.writeAsText(params.get("output") + "Query-Flink-Result", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);

		// Ejecutar el job de Flink
		env.execute("SPARQL Query to Flink Program - DataSet API");
	}
}
