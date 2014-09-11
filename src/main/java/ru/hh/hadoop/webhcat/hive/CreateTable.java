package ru.hh.hadoop.webhcat.hive;

import java.util.LinkedList;
import java.util.List;

public class CreateTable {
  Boolean external;
  String location;
  List<Column> columns = new LinkedList<>();
  TableFormat format;

  CreateTable() {
  }

  public Boolean getExternal() {
    return external;
  }

  public String getLocation() {
    return location;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public TableFormat getFormat() {
    return format;
  }

  public static CreateTableBuilder builder(){
    return new CreateTableBuilder(new CreateTable());
  }
}
