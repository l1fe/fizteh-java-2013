package ru.fizteh.fivt.students.inaumov.storeable.base;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.inaumov.filemap.base.AbstractDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.ReadHandler;
import ru.fizteh.fivt.students.inaumov.multifilemap.MultiFileMapUtils;
import ru.fizteh.fivt.students.inaumov.multifilemap.handlers.LoadHandler;
import ru.fizteh.fivt.students.inaumov.multifilemap.handlers.SaveHandler;
import ru.fizteh.fivt.students.inaumov.storeable.StoreableUtils;
import ru.fizteh.fivt.students.inaumov.storeable.builders.StoreableTableBuilder;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Set;

import static ru.fizteh.fivt.students.inaumov.storeable.StoreableUtils.isStringIncorrect;
import static ru.fizteh.fivt.students.inaumov.storeable.StoreableUtils.writeSizeSignature;

public class DatabaseTable extends AbstractDatabaseTable<String, Storeable> implements Table {
    private DatabaseTableProvider tableProvider;
    private List<Class<?>> columnTypes;

    public DatabaseTable(DatabaseTableProvider tableProvider, String databaseDirectory, String tableName,
                         List<Class<?>> columnTypes) {
        super(databaseDirectory, tableName);

        if (columnTypes == null || columnTypes.isEmpty()) {
            throw new IllegalArgumentException("error: column types can't be null");
        }

        this.columnTypes = columnTypes;
        this.tableProvider = tableProvider;

        try {
            checkTableDir();
            loadTable();
        } catch (IOException e) {
            throw new IllegalArgumentException("error: incorrect file format");
        }
    }

    public DatabaseTable(DatabaseTable otherTable) {
        super(otherTable.getDir(), otherTable.rawGetTableName());
        this.columnTypes = otherTable.columnTypes;
        this.tableProvider = otherTable.tableProvider;
        this.keyValueHashMap = otherTable.keyValueHashMap;
    }

    @Override
    public Storeable put(String key, Storeable value) throws ColumnFormatException {
        if (key != null) {
            if (isStringIncorrect(key)) {
                throw new IllegalArgumentException("error: key can't contain whitespaces or be empty");
            }
        }
        if (value == null) {
            throw new IllegalArgumentException("error: value can't be null");
        }
        if (!checkAlienStoreable(value)) {
            throw new ColumnFormatException("error: alien storeable");
        }

        checkCorrectStoreable(value);

        Storeable oldValue;
        try {
            oldValue = tablePut(key, value);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return oldValue;
    }

    @Override
    public Storeable get(String key) {
        Storeable value;
        try {
            value = tableGet(key);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return value;
    }

    @Override
    public Storeable remove(String key) {
        Storeable value;
        try {
            value = tableRemove(key);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return value;
    }

    @Override
    public int size() {
        int tableSize;
        try {
            tableSize = tableSize();
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return tableSize;
    }

    @Override
    public int commit() throws IOException {
        int committedChangesNumber = tableCommit();
        writeSizeSignatureFile(tableSize());

        return committedChangesNumber;
    }

    @Override
    public int rollback() {
        int uncommittedChangesNumber;
        try {
            uncommittedChangesNumber = tableRollback();
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return uncommittedChangesNumber;
    }

    @Override
    public int getColumnsCount() {
        tableState.checkAvailable();

        return columnTypes.size();
    }

    @Override
    public Class<?> getColumnType(int columnIndex) {
        tableState.checkAvailable();

        if (columnIndex < 0 || columnIndex > getColumnsCount()) {
            throw new IndexOutOfBoundsException();
        }

        return columnTypes.get(columnIndex);
    }

    @Override
    protected void loadTable() throws IOException {
        if (tableProvider == null) {
            return;
        }

        //LoadHandler.loadTable(new StoreableTableBuilder(tableProvider, this));
    }

    @Override
    protected void loadTableLazy(String key) throws IOException {
        String tableDir = getTableDir();
        Integer dirNumber = MultiFileMapUtils.getDirNumber(key);
        Integer fileNumber = MultiFileMapUtils.getFileNumber(key);

        File bucket = new File(tableDir, dirNumber.toString() + ".dir");
        File file = new File(bucket, fileNumber.toString() + ".dat");

        if (!file.exists()) {
            return;
        }

        int currentBucketNumber = MultiFileMapUtils.parseCurrentBucketNumber(bucket);
        int currentFileNumber = MultiFileMapUtils.parseCurrentFileNumber(file);
        if (dirNumber != currentBucketNumber || fileNumber != currentFileNumber) {
            throw new IllegalArgumentException("error: illegal key placement");
        }

        StoreableTableBuilder builder = new StoreableTableBuilder(tableProvider, this);
        builder.setCurrentFile(file);
        ReadHandler.loadFromFile(file.getAbsolutePath(), builder);
    }

    @Override
    public void saveTable() throws IOException {
        SaveHandler.saveTable(new StoreableTableBuilder(tableProvider, this));
    }

    private void checkTableDir() throws IOException {
        File tableDirectory = new File(getDir(), getName());
        if (!tableDirectory.exists()) {
            tableDirectory.mkdir();
            writeTypesSignatureFile();
            writeSizeSignatureFile(0);
        } else {
            File[] children = tableDirectory.listFiles();
            if (children == null || children.length == 0) {
                throw new IllegalArgumentException("error: table directory: "
                        + tableDirectory.getAbsolutePath() + " is empty");
            }
        }
    }

    private void writeSizeSignatureFile(int size) throws IOException {
        File tableDir = new File(getDir(), getName());
        File signatureFile = new File(tableDir, DatabaseTableProvider.SIZE_SIGNATURE_FILE_NAME);
        StoreableUtils.writeSizeSignature(signatureFile, size);
    }

    private void writeTypesSignatureFile() throws IOException {
        File tableDir = new File(getDir(), getName());
        File signatureFile = new File(tableDir, DatabaseTableProvider.TYPES_SIGNATURE_FILE_NAME);
        StoreableUtils.writeTypesSignature(signatureFile, columnTypes);
    }

    public boolean checkAlienStoreable(Storeable storeable) {
        for (int i = 0; i < getColumnsCount(); ++i) {
            try {
                Object obj = storeable.getColumnAt(i);
                if (obj == null) {
                    continue;
                }
                if (!obj.getClass().equals(getColumnType(i))) {
                    return false;
                }
            } catch (IndexOutOfBoundsException e) {
                return false;
            }
        }

        try {
            storeable.getColumnAt(getColumnsCount());
        } catch (IndexOutOfBoundsException e) {
            return true;
        }

        return false;
    }

    public void checkCorrectStoreable(Storeable storeable) {
        for (int i = 0; i < getColumnsCount(); ++i) {
            try {
                StoreableUtils.isValueCorrect(storeable.getColumnAt(i), columnTypes.get(i));
            } catch (ParseException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public Set<String> rawGetKeys() {
        return keyValueHashMap.keySet();
    }

    public String getTableDir() {
        File tableDir = new File(getDir(), getName());
        return tableDir.getAbsolutePath();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getTableDir() + "]";
    }
}
