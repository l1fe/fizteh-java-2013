package ru.fizteh.fivt.students.inaumov.storeable.base;

import org.json.JSONArray;
import org.json.JSONException;
import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.storage.structured.TableProvider;
import ru.fizteh.fivt.students.inaumov.filemap.base.TableState;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.ReadHandler;
import ru.fizteh.fivt.students.inaumov.multifilemap.MultiFileMapUtils;
import ru.fizteh.fivt.students.inaumov.multifilemap.handlers.LoadHandler;
import ru.fizteh.fivt.students.inaumov.storeable.StoreableUtils;
import ru.fizteh.fivt.students.inaumov.storeable.TypesFormatter;
import ru.fizteh.fivt.students.inaumov.storeable.builders.StoreableTableBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DatabaseTableProvider implements TableProvider, AutoCloseable {
    static final String TYPES_SIGNATURE_FILE_NAME = "signature.tsv";
    static final String SIZE_SIGNATURE_FILE_NAME = "size.tsv";

    private static final String CHECK_EXPRESSION_STRING = "[0-9A-Za-zА-Яа-я]+";
    private final Lock tableLock = new ReentrantLock(true);

    private HashMap<String, DatabaseTable> tables = new HashMap<String, DatabaseTable>();
    private String databaseDirectoryPath;
    private DatabaseTable currentTable = null;
    protected TableState state;

    public DatabaseTableProvider(String databaseDirectoryPath) {
        if (databaseDirectoryPath == null) {
            throw new IllegalArgumentException("error: database directory can't be null");
        }

        this.databaseDirectoryPath = databaseDirectoryPath;

        state = TableState.WORKING;

        File databaseDirectory = new File(databaseDirectoryPath);
        if (databaseDirectory.isFile()) {
            throw new IllegalArgumentException("error: database can't be placed in a file");
        }

        for (File tableFile: databaseDirectory.listFiles()) {
            if (tableFile.isFile()) {
                continue;
            }

            List<Class<?>> columnTypes = readTableTypesSignature(tableFile.getName());

            if (columnTypes == null) {
                throw new IllegalArgumentException("error: table directory can't be empty");
            }

            DatabaseTable table = new DatabaseTable(this, databaseDirectoryPath, tableFile.getName(), columnTypes);
            tables.put(table.getName(), table);
        }
    }

    @Override
    public Table getTable(String name) {
        try {
            tableLock.lock();

            state.checkAvailable();

            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("error: table name can't be null (or empty)");
            }

            checkTableName(name);

            DatabaseTable table = tables.get(name);

            if (table == null) {
                return null;
            }

            if (currentTable != null && currentTable.getUnsavedChangesNumber() > 0) {
                throw new IllegalStateException(currentTable.getUnsavedChangesNumber() + " unsaved changes");
            }

            if (table.isClosed()) {
                table = new DatabaseTable(table);
                tables.put(table.getName(), table);
            }

            File sizeSignatureFile = new File(table.getTableDir(), DatabaseTableProvider.SIZE_SIGNATURE_FILE_NAME);
            if (!sizeSignatureFile.exists()) {
                try {
                    int recordsNumber = StoreableUtils.countRecords(table.getTableDir());
                    table.size = recordsNumber;
                    StoreableUtils.writeSizeSignature(sizeSignatureFile, table.tableSize());
                } catch (IOException e) {
                    throw new IllegalStateException("error: can't write size signature file");
                }
            } else {
                try {
                    int tableSize = StoreableUtils.getSizeFromSizeSignature(sizeSignatureFile);
                    table.size = tableSize;
                } catch (IOException e) {
                    throw new IllegalStateException("error: can't read size signature file");
                }
            }

            currentTable = table;
            return table;
        } finally {
            tableLock.unlock();
        }
    }

    @Override
    public Table createTable(String name, List<Class<?>> columnTypes) throws IOException {
        try {
            tableLock.lock();

            state.checkAvailable();

            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("error: table name can't be null (or empty)");
            }

            checkTableName(name);

            if (columnTypes == null || columnTypes.isEmpty()) {
                throw new IllegalArgumentException("error: wrong type (null or empty columns types)");
            }

            checkColumnTypes(columnTypes);

            if (tables.containsKey(name)) {
                return null;
            }

            File tableDir = new File(databaseDirectoryPath, name);

            File typesSignatureFile = new File(tableDir, DatabaseTableProvider.TYPES_SIGNATURE_FILE_NAME);
            StoreableUtils.writeTypesSignature(typesSignatureFile, columnTypes);

            File sizeSignatureFile = new File(tableDir, DatabaseTableProvider.SIZE_SIGNATURE_FILE_NAME);
            StoreableUtils.writeSizeSignature(sizeSignatureFile, 0);

            DatabaseTable table = new DatabaseTable(this, databaseDirectoryPath, name, columnTypes);
            tables.put(name, table);

            return table;
        } finally {
            tableLock.unlock();
        }
    }

    @Override
    public void removeTable(String name) throws IOException {
        try {
            tableLock.lock();

            state.checkAvailable();

            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("error: table name can't be null (or empty)");
            }

            if (!tables.containsKey(name)) {
                throw new IllegalStateException(name + " not exists");
            }

            tables.remove(name);

            File tableFile = new File(databaseDirectoryPath, name);
            MultiFileMapUtils.deleteFile(tableFile);

            currentTable = null;
        } finally {
            tableLock.unlock();
        }
    }

    @Override
    public String serialize(Table table, Storeable value) throws ColumnFormatException {
        state.checkAvailable();

        Object[] values = new Object[table.getColumnsCount()];
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            values[i] = value.getColumnAt(i);
        }

        JSONArray array = new JSONArray(values);
        return array.toString();
    }

    @Override
    public Storeable deserialize(Table table, String value) throws ParseException {
        state.checkAvailable();

        JSONArray array;
        try {
            array = new JSONArray(value);
        } catch (JSONException e) {
            throw new ParseException("error: can't resolve input string", -1);
        }

        if (array.length() != table.getColumnsCount()) {
            throw new ParseException("error: incorrect input columns count", -1);
        }

        List<Object> values = new ArrayList<Object>();
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            if (array.get(i).equals(null)) {
                values.add(null);
            } else if (array.get(i).getClass() == Integer.class && table.getColumnType(i) == Integer.class) {
                values.add(array.getInt(i));
            } else if ((array.get(i).getClass() == Long.class || array.get(i).getClass() == Integer.class)
                    && table.getColumnType(i) == Long.class) {
                values.add(array.getLong(i));
            } else if (array.get(i).getClass() == Integer.class && table.getColumnType(i) == Byte.class) {
                Integer a = array.getInt(i);
                values.add(a.byteValue());
            } else if (array.get(i).getClass() == Double.class && table.getColumnType(i) == Float.class) {
                Double a = array.getDouble(i);
                values.add(a.floatValue());
            } else if (array.get(i).getClass() == Double.class && table.getColumnType(i) == Double.class) {
                values.add(array.getDouble(i));
            } else if (array.get(i).getClass() == Boolean.class && table.getColumnType(i) == Boolean.class) {
                values.add(array.getBoolean(i));
            } else if (array.get(i).getClass() == String.class && table.getColumnType(i) == String.class) {
                values.add(array.getString(i));
            } else {
                throw new ParseException("Incorrect value string.", -1);
            }
        }

        return createFor(table, values);
    }

    @Override
    public Storeable createFor(Table table) {
        state.checkAvailable();

        return rawCreateFor(table);
    }

    @Override
    public Storeable createFor(Table table, List<?> values) {
        state.checkAvailable();

        if (values == null) {
            throw new IllegalArgumentException("error: values can't be null");
        }

        DatabaseRow row = rawCreateFor(table);
        row.setColumns(values);

        return row;
    }

    private List<Class<?>> readTableTypesSignature(String tableName) {
        File tableDirectory = new File(databaseDirectoryPath, tableName);
        File signatureFile = new File(tableDirectory, TYPES_SIGNATURE_FILE_NAME);

        String signature = null;
        if (!signatureFile.exists()) {
            return null;
        }

        try {
            BufferedReader reader = new BufferedReader(new FileReader(signatureFile));
            signature = reader.readLine();
        } catch (IOException e) {
            System.err.println("error: can't load signature file: " + e.getMessage());
            return null;
        }

        if (signature == null) {
            throw new IllegalArgumentException("error: incorrect signature file");
        }

        List<Class<?>> columnTypes = new ArrayList<Class<?>>();
        for (final String columnType: signature.split("\\s+")) {
            Class<?> type = TypesFormatter.getTypeByName(columnType);
            if (type == null) {
                throw new IllegalArgumentException("unknown type");
            }

            columnTypes.add(type);
        }

        return columnTypes;
    }

    private DatabaseRow rawCreateFor(Table table) {
        DatabaseRow row = new DatabaseRow();
        for (int i = 0; i < table.getColumnsCount(); ++i) {
            row.addColumn(table.getColumnType(i));
        }

        return row;
    }

    private void checkColumnTypes(List<Class<?>> columnTypes) {
        for (final Class<?> columnType : columnTypes) {
            if (columnType == null) {
                throw new IllegalArgumentException("unknown column type");
            }

            TypesFormatter.getSimpleName(columnType);
        }
    }

    private void checkTableName(String name) {
        if (!name.matches(CHECK_EXPRESSION_STRING)) {
            throw new IllegalArgumentException("error: bad table name");
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + databaseDirectoryPath + "]";
    }

    @Override
    public void close() throws Exception {
        if (state.equals(TableState.CLOSED)) {
            return;
        }

        for (final String tableName : tables.keySet()) {
            tables.get(tableName).close();
        }

        state = TableState.CLOSED;
    }
}
