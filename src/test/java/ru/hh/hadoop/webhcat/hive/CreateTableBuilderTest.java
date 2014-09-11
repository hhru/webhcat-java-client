package ru.hh.hadoop.webhcat.hive;

import org.junit.Test;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CreateTableBuilderTest {

  @Test
  public void shouldCreateMinimalConfigurationTable() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").build();

    assertNull(createTable.getExternal());
    assertNull(createTable.getLocation());
    assertNull(createTable.getFormat());

    final List<Column> columns = createTable.getColumns();
    assertEquals(1, columns.size());

    assertEquals("id", columns.get(0).getName());
    assertEquals("int", columns.get(0).getType());
    assertNull(columns.get(0).getComment());
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowExceptionWhenColumnsWasNotDefined() throws Exception {
    CreateTable.builder().build();
  }

  @Test
  public void shouldNotFillRowFormatWhenSetStoredAs() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").storedAs("TEXTFILE").build();

    final TableFormat tableFormat = createTable.getFormat();
    assertNull(tableFormat.getRowFormat());

    assertEquals("TEXTFILE", tableFormat.getStoredAs());
  }

  @Test
  public void shouldFillFieldsTerminatedByAndNotFillOtherFormatFields() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").fieldsTerminatedBy("\\n").build();

    final TableFormat tableFormat = createTable.getFormat();
    assertNull(tableFormat.getStoredAs());

    final TableFormat.RowFormat rowFormat = tableFormat.getRowFormat();
    assertEquals("\\n", rowFormat.getFieldsTerminatedBy());

    assertNull(rowFormat.getCollectionItemsTerminatedBy());
    assertNull(rowFormat.getLinesTerminatedBy());
    assertNull(rowFormat.getMapKeysTerminatedBy());
  }

  @Test
  public void shouldFillCollectionItemsTerminatedByAndNotFillOtherFormatFields() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").collectionTerminatedBy('\n').build();

    final TableFormat tableFormat = createTable.getFormat();
    assertNull(tableFormat.getStoredAs());

    final TableFormat.RowFormat rowFormat = tableFormat.getRowFormat();
    assertEquals('\n', rowFormat.getCollectionItemsTerminatedBy().charValue());

    assertNull(rowFormat.getFieldsTerminatedBy());
    assertNull(rowFormat.getLinesTerminatedBy());
    assertNull(rowFormat.getMapKeysTerminatedBy());
  }

  @Test
  public void shouldFillLinesTerminatedByAndNotFillOtherFormatFields() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").linesTerminatedBy('\n').build();

    final TableFormat tableFormat = createTable.getFormat();
    assertNull(tableFormat.getStoredAs());

    final TableFormat.RowFormat rowFormat = tableFormat.getRowFormat();
    assertEquals('\n', rowFormat.getLinesTerminatedBy().charValue());

    assertNull(rowFormat.getFieldsTerminatedBy());
    assertNull(rowFormat.getCollectionItemsTerminatedBy());
    assertNull(rowFormat.getMapKeysTerminatedBy());
  }

  @Test
  public void shouldFillMapKeysTerminatedByAndNotFillOtherFormatFields() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").mapKeysTerminatedBy('\n').build();

    final TableFormat tableFormat = createTable.getFormat();
    assertNull(tableFormat.getStoredAs());

    final TableFormat.RowFormat rowFormat = tableFormat.getRowFormat();
    assertEquals('\n', rowFormat.getMapKeysTerminatedBy().charValue());

    assertNull(rowFormat.getFieldsTerminatedBy());
    assertNull(rowFormat.getCollectionItemsTerminatedBy());
    assertNull(rowFormat.getLinesTerminatedBy());
  }

  @Test
  public void shouldFillLocationForExternalTable() throws Exception {
    final CreateTable createTable = CreateTable.builder().addColumn("id", "int").external("tableLocation").build();

    assertTrue(createTable.getExternal());
    assertEquals("tableLocation", createTable.getLocation());
  }
}
