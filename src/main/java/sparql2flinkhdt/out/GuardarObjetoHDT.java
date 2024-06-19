package sparql2flinkhdt.out;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.rdfhdt.hdt.hdt.HDT;
import sparql2flinkhdt.runner.LoadTriples;
import sparql2flinkhdt.runner.SerializableHDT;
import java.io.*;

public class GuardarObjetoHDT implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(GuardarObjetoHDT.class);

	public static void main(String[] args) throws Exception {

		try {
			LOG.info("Inicio de la aplicación Henrry");
			final ParameterTool params = ParameterTool.fromArgs(args);

			if (!params.has("dataset") && !params.has("output")) {
				System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
				LOG.error("Use --dataset to specify dataset path and use --output to specify output path.");
				return;
			}

			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

				DataSet<String> rdfDataSet = env.readTextFile(params.get("dataset"));

			HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));

			String outputFile = params.get("output");

			try {
				SerializableHDT serializableHDT = new SerializableHDT(hdt.getHeader(), hdt.getDictionary(), hdt.getTriples());
				saveSerializableHDTToFile(serializableHDT, outputFile);
				System.out.println("HDT guardado exitosamente");
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Error al guardar el archivo HDT");
			}

			// Agregar operación de escritura para los datos procesados
			rdfDataSet.writeAsText("path/to/intermediate/output.txt");

			// Ejecutar el entorno de Flink
			env.execute("SPARQL Query to Flink Program - DataSet API");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void saveSerializableHDTToFile(SerializableHDT serializableHDT, String fileName) throws IOException {
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
			oos.writeObject(serializableHDT);
		}
	}
}



//package sparql2flinkhdt.out;
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.rdfhdt.hdt.hdt.HDT;
//
//import java.io.*;
//
//import sparql2flinkhdt.runner.LoadTriples;
//
//import sparql2flinkhdt.runner.SerializableHDT;
//
//public class GuardarObjetoHDT implements Serializable {
//	private static final Logger LOG = LoggerFactory.getLogger(GuardarObjetoHDT.class);
//
//	public static void main(String[] args) throws Exception {
//
//		try {
//
//			LOG.info("Inicio de la aplicación Henrry");
//			final ParameterTool params = ParameterTool.fromArgs(args);
//
//			if (!params.has("dataset") && !params.has("output")) {
//				System.out.println("Use --dataset to specify dataset path and use --output to specify output path.");
//				LOG.error("Use --dataset to specify dataset path and use --output to specify output path.");
//				return;
//			}
//
//			//************ Environment (DataSet) and Source (static RDF dataset) ************
//			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//			HDT hdt = LoadTriples.fromDataset(env, params.get("dataset"));
//
//			String outputFile = "./examples/output.hdt";
//
//			try {
//				SerializableHDT serializableHDT = new SerializableHDT(hdt.getHeader(), hdt.getDictionary(),hdt.getTriples());
//				saveSerializableHDTToFile(serializableHDT, outputFile);
//				System.out.println("HDT guardado exitosamente");
//			} catch (Exception e) {
//				e.printStackTrace();
//				System.err.println("Error al guardar el archivo HDT");
//			}
//			// Agregar operación de escritura
//			hdt.saveToHDT(outputFile, null); // Utiliza null para el ProgressListener si no necesitas uno
//			env.execute("SPARQL Query to Flink Programan - DataSet API");
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//	private static void saveSerializableHDTToFile (SerializableHDT serializableHDT, String fileName) throws
//			IOException {
//		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName))) {
//			oos.writeObject(serializableHDT);
//		}
//	}
//}