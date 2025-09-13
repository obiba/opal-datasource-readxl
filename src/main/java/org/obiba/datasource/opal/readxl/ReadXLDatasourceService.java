package org.obiba.datasource.opal.readxl;

import com.google.common.collect.Lists;
import org.json.JSONObject;
import org.obiba.magma.Datasource;
import org.obiba.magma.DatasourceFactory;
import org.obiba.magma.ValueTable;
import org.obiba.magma.support.StaticDatasource;
import org.obiba.opal.spi.datasource.DatasourceUsage;
import org.obiba.opal.spi.r.FolderReadROperation;
import org.obiba.opal.spi.r.RUtils;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceFactory;
import org.obiba.opal.spi.r.datasource.AbstractRDatasourceService;
import org.obiba.opal.spi.r.datasource.RDatasourceFactory;
import org.obiba.opal.spi.r.datasource.magma.RDatasource;
import org.obiba.opal.spi.r.datasource.magma.RSymbolWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;

public class ReadXLDatasourceService extends AbstractRDatasourceService {

  private static final Logger log = LoggerFactory.getLogger(ReadXLDatasourceService.class);

  @Override
  public String getName() {
    return "opal-datasource-readxl";
  }

  @Override
  public DatasourceFactory createDatasourceFactory(DatasourceUsage usage, JSONObject parameters) {
    switch (usage) {
      case IMPORT:
        return createImportDatasourceFactory(parameters);
      case EXPORT:
        return createExportDatasourceFactory(parameters);
    }
    throw new NoSuchMethodError("Datasource usage not available: " + usage);
  }

  private DatasourceFactory createImportDatasourceFactory(JSONObject parameters) {
    RDatasourceFactory factory = new AbstractRDatasourceFactory() {
      @NotNull
      @Override
      protected Datasource internalCreate() {
        File file = resolvePath(parameters.optString("file"));

        String missingValuesCharacters = parameters.optString("na", "\"\", \"NA\"");
        int skip = parameters.optInt("skip");

        // copy file to the R session
        prepareFile(file);
        DataReadXLOperation readOp = new DataReadXLOperation(file.getName(), missingValuesCharacters, skip);
        execute(readOp);
        return new RDatasource(getName(), getRSessionHandler(), readOp.getSymbols(), parameters.optString("entity_type"),
            parameters.optString("id"));
      }
    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }

  private DatasourceFactory createExportDatasourceFactory(JSONObject parameters) {
    RDatasourceFactory factory = new AbstractRDatasourceFactory() {
      @NotNull
      @Override
      protected Datasource internalCreate() {
        return new StaticDatasource(getOutputFile().getName());
      }

      @Override
      public RSymbolWriter createSymbolWriter() {

        return new RSymbolWriter() {

          private final List<String> symbols = Lists.newArrayList();

          @Override
          public String getSymbol(ValueTable table) {
            return RUtils.getSymbol(table.getName());
          }

          @Override
          public void write(ValueTable table) {
            String symbol = getSymbol(table);
            symbols.add(symbol);
          }

          @Override
          public void dispose() {
            if (symbols.isEmpty()) return;
            execute(new DataWriteXLOperation(symbols, "out.xlsx"));

            // copy file from R session
            File outputFolder = getOutputFile();
            String symbol = symbols.getFirst();
            String resultFileName = symbol + ".xlsx";
            File resultFile = Paths.get(outputFolder.getAbsolutePath(), resultFileName).toFile();
            int i = 1;
            while (resultFile.exists()) {
              resultFileName = symbol + "-" + i + ".xlsx";
              resultFile = Paths.get(outputFolder.getAbsolutePath(), resultFileName).toFile();
              i++;
            }
            execute(new FolderReadROperation(outputFolder));
          }
        };
      }

      private File getOutputFile() {
        return resolvePath(parameters.optString("file"));
      }

    };
    factory.setRSessionHandler(getRSessionHandler());
    return factory;
  }
}
