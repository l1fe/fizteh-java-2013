package ru.fizteh.fivt.students.eltyshev.storable.database;

import ru.fizteh.fivt.storage.structured.*;
import ru.fizteh.fivt.students.eltyshev.filemap.base.AbstractStorage;
import ru.fizteh.fivt.students.eltyshev.multifilemap.DistributedLoader;
import ru.fizteh.fivt.students.eltyshev.multifilemap.DistributedSaver;
import ru.fizteh.fivt.students.eltyshev.storable.StoreableUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DatabaseTable extends AbstractStorage<String, Storeable> implements Table {
    DatabaseTableProvider provider;

    private List<Class<?>> columnTypes;

    public DatabaseTable(DatabaseTableProvider provider, String databaseDirectory, String tableName, List<Class<?>> columnTypes) {
        super(databaseDirectory, tableName);
        if (columnTypes == null || columnTypes.isEmpty()) {
            throw new IllegalArgumentException("column types cannot be null");
        }
        this.columnTypes = columnTypes;
        this.provider = provider;

        try {
            checkTableDirectory();
            load();
        } catch (IOException e) {
            System.err.println("error loading table: " + e.getMessage());
        }
    }

    @Override
    public Storeable get(String key) {
        return storageGet(key);
    }

    @Override
    public Storeable put(String key, Storeable value) throws ColumnFormatException {
        if (key != null) {
            if (key.trim().isEmpty()) {
                throw new IllegalArgumentException("key cannot be empty");
            }
        }
        return storagePut(key, value);
    }

    @Override
    public Storeable remove(String key) {
        return storageRemove(key);
    }

    @Override
    public int size() {
        return storageSize();
    }

    @Override
    public int commit() throws IOException {
        return storageCommit();
    }

    @Override
    public int rollback() {
        return storageRollback();
    }

    @Override
    public int getColumnsCount() {
        return columnTypes.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) throws IndexOutOfBoundsException {
        if (columnIndex < 0 || columnIndex > getColumnsCount()) {
            throw new IndexOutOfBoundsException();
        }
        return columnTypes.get(columnIndex);
    }

    @Override
    protected void load() throws IOException {
        if (provider == null) {
            return;
        }
        DistributedLoader.load(new StoreableTableBuilder(provider, this));
    }

    @Override
    protected void save() throws IOException {
        DistributedSaver.save(new StoreableTableBuilder(provider, this));
    }

    private void checkTableDirectory() throws IOException {
        File tableDirectory = new File(getDirectory(), getName());
        if (!tableDirectory.exists()) {
            tableDirectory.mkdir();
            writeSignatureFile();
        } else {
            File[] children = tableDirectory.listFiles();
            if (children == null || children.length == 0) {
                throw new IllegalArgumentException(String.format("table directory: %s is empty", tableDirectory.getAbsolutePath()));
            }
        }
    }

    private void writeSignatureFile() throws IOException {
        File tableDirectory = new File(getDirectory(), getName());
        File signatureFile = new File(tableDirectory, DatabaseTableProvider.SIGNATURE_FILE);
        signatureFile.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(signatureFile));
        List<String> formattedColumnTypes = StoreableUtils.formatColumnTypes(columnTypes);
        String signature = StoreableUtils.join(formattedColumnTypes);
        writer.write(signature);
        writer.close();
    }

    Set<String> rawGetKeys() {
        return oldData.keySet();
    }
}
