package org.obiba.datasource.opal.readxl;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.obiba.opal.spi.r.AbstractROperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class DataWriteXLOperation extends AbstractROperation {

  private static final Logger log = LoggerFactory.getLogger(DataWriteXLOperation.class);

  private final List<String> symbols;

  private final String destination;

  public DataWriteXLOperation(List<String> symbols, String destination) {
    this.symbols = symbols;
    this.destination = destination;
  }

  @Override
  protected void doWithConnection() {
    if (Strings.isNullOrEmpty(destination)) return;
    ensurePackage("writexl");
    eval("library(writexl)", false);
    ensurePackage("tibble");
    eval("library(tibble)", false);

    String writeCmd = writeSheets();
    log.debug("Eval command: {}", writeCmd);
    eval(writeCmd, false);
  }

  private String writeSheets() {
    return String.format("write_xlsx(list(%s), \"%s\")", getSymbolsString(), destination);
  }

  private String getSymbolsString() {
    return Joiner.on(", ").join(symbols.stream().map(s -> String.format("\"%s\"=`%s`", s, s)).collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return writeSheets();
  }
}
