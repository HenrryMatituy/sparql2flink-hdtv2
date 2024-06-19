package sparql2flinkhdt.runner;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.thrift.TSerializable;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class LoadTriples implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTriples.class);
    public static HDT fromDataset(ExecutionEnvironment environment, String filePath) {Preconditions.checkNotNull(filePath, "The file path may not be null...");
        HDT hdt = null;


        try {
            // Log para verificar que la ruta del archivo es la correcta
            LOG.info("Intentando generar HDT desde el archivo: {}", filePath);

            // Intenta generar HDT y manejar cualquier excepción
            hdt = HDTManager.generateHDT(filePath, "http://example.org/baseURI", RDFNotation.parse("ntriples"), new HDTSpecification(), null);

            // Log para verificar que la generación fue exitosa
            LOG.info("Generación de HDT exitosa.");

        } catch (Exception e) {
            // Log para informar sobre cualquier excepción
            LOG.error("Error al generar HDT desde el archivo: {}", filePath, e);
        }

        return hdt;
    }
}

