package ru.hh.hadoop.webhcat.hive;

public class TableFormat {

  String storedAs;
  RowFormat rowFormat;

  TableFormat() {
  }

  public String getStoredAs() {
    return storedAs;
  }

  public RowFormat getRowFormat() {
    return rowFormat;
  }

  public static class RowFormat {
    String fieldsTerminatedBy;
    Character collectionItemsTerminatedBy;
    Character mapKeysTerminatedBy;
    Character linesTerminatedBy;

    RowFormat() {
    }

    public String getFieldsTerminatedBy() {
      return fieldsTerminatedBy;
    }

    public Character getCollectionItemsTerminatedBy() {
      return collectionItemsTerminatedBy;
    }

    public Character getMapKeysTerminatedBy() {
      return mapKeysTerminatedBy;
    }

    public Character getLinesTerminatedBy() {
      return linesTerminatedBy;
    }
  }
}
