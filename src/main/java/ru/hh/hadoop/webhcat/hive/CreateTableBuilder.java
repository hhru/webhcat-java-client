package ru.hh.hadoop.webhcat.hive;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class CreateTableBuilder {
  private CreateTable createTable;

  CreateTableBuilder(CreateTable createTable) {
    this.createTable = createTable;
  }

  public CreateTableBuilder addColumn(String name, String type) {
    return addColumn(name, type, null);
  }

  public CreateTableBuilder addColumn(String name, String type, String comment) {
    createTable.columns.add(new Column(name, type, comment));

    return this;
  }

  public CreateTableBuilder external(String location) {
    createTable.external = true;
    createTable.location = location;

    return this;
  }

  public CreateTableBuilder storedAs(String fileFormat) {
    createTableFormatIfNeed();
    createTable.format.storedAs = fileFormat;

    return this;
  }

  public CreateTableBuilder fieldsTerminatedBy(String fieldTerminator) {
    createRowFormatIfNeed();
    createTable.format.rowFormat.fieldsTerminatedBy = fieldTerminator;
    return this;
  }

  public CreateTableBuilder collectionTerminatedBy(char collectionTerminator) {
    createRowFormatIfNeed();
    createTable.format.rowFormat.collectionItemsTerminatedBy = collectionTerminator;
    return this;
  }

  public CreateTableBuilder linesTerminatedBy(char linesTerminator) {
    createRowFormatIfNeed();
    createTable.format.rowFormat.linesTerminatedBy = linesTerminator;
    return this;
  }

  public CreateTableBuilder mapKeysTerminatedBy(char mapKeysTerminator) {
    createRowFormatIfNeed();
    createTable.format.rowFormat.mapKeysTerminatedBy = mapKeysTerminator;
    return this;
  }

  private void createTableFormatIfNeed() {
    if (createTable.format == null) {
      createTable.format = new TableFormat();
    }
  }

  private void createRowFormatIfNeed() {
    createTableFormatIfNeed();

    if (createTable.format.rowFormat == null) {
      createTable.format.rowFormat = new TableFormat.RowFormat();
    }
  }

  public CreateTable build() {
    if (createTable.columns.size() == 0) {
      throw new IllegalStateException("Columns was not defined");
    }

    if (createTable.external != null && StringUtils.isBlank(createTable.location)) {
      throw new IllegalStateException("Location not set for external table");
    }

    return createTable;
  }
}
