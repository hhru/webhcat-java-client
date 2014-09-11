package ru.hh.hadoop.webhcat.hive;

public class Column {
  private String name;
  private String type;
  private String comment;

  Column() {
  }

  Column(String name, String type, String comment) {
    this.name = name;
    this.type = type;
    this.comment = comment;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getComment() {
    return comment;
  }
}
