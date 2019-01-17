package org.obiba.datasource.opal.readxl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Strings;

import com.google.common.collect.Lists;
import org.obiba.opal.spi.r.AbstractROperation;
import org.obiba.opal.spi.r.RUtils;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataReadXLOperation extends AbstractROperation {

  private static final Logger log = LoggerFactory.getLogger(DataReadXLOperation.class);

  private final String source;

  private final String missingValuesCharacters;

  private final int numberOfRecordsToSkip;

  private final List<String> symbols = Lists.newArrayList();

  public DataReadXLOperation(String source, String missingValuesCharacters, int numberOfRecordsToSkip) {
    this.source = source;
    this.missingValuesCharacters = missingValuesCharacters;
    this.numberOfRecordsToSkip = numberOfRecordsToSkip < 0 ? 0 : numberOfRecordsToSkip;
  }

  public List<String> getSymbols() {
    return symbols;
  }

  @Override
  protected void doWithConnection() {
    if(Strings.isNullOrEmpty(source)) return;
    ensurePackage("readxl");
    eval("library(readxl)", false);
    ensurePackage("tibble");
    eval("library(tibble)", false);

    // list sheet names
    try {
      String[] names = eval(String.format("excel_sheets(\"%s\")", source), false).asStrings();

      for (String sheet : names) {
        String readCmd = readSheet(sheet);
        log.debug("Eval command: {}", readCmd);
        eval(readCmd, false);
      }
    } catch (REXPMismatchException e) {
      log.warn("Unable to read Excel sheet names", e);
    }
  }

  private String readSheet(String sheet) {
    String symbol = RUtils.getSymbol(sheet);
    symbols.add(symbol);
    return String.format("is.null(base::assign(\"%s\", read_excel(\"%s\", sheet=\"%s\", %s, %s)))", symbol, source, sheet, missingValues(), numberOfRecordsToSkipValue());
  }

  private String missingValues() {
    if (!Strings.isNullOrEmpty(missingValuesCharacters)) {
      return String.format("na = c(%s)", Stream.of(missingValuesCharacters.split(",")).map(s -> "\"" + s.replace("\"", "").replace("'", "\\'") + "\"").collect(Collectors.joining(",")));
    }

    return "na = c(\"\", \"NA\")";
  }

  private String numberOfRecordsToSkipValue() {
    return "skip = " + numberOfRecordsToSkip;
  }

  @Override
  public String toString() {
    return readSheet("?");
  }
}
