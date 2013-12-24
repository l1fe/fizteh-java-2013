package ru.fizteh.fivt.students.inaumov.filemap.base;

import ru.fizteh.fivt.storage.strings.Table;

import java.io.IOException;

public abstract class StringDatabaseTable extends AbstractDatabaseTable<String, String> implements Table {
    public StringDatabaseTable(String dir, String tableName) {
        super(dir, tableName);
    }

    @Override
    public String get(String key) {
        String value;
        try {
            value = tableGet(key);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return value;
    }

    @Override
    public String put(String key, String value) {
        String oldValue;
        try {
            oldValue = tablePut(key, value);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return oldValue;
    }

    @Override
    public String remove(String key) {
        String oldValue;
        try {
            oldValue = tableRemove(key);
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return oldValue;
    }

    @Override
    public int commit() {
        int commitedChangesNumber;
        try {
            commitedChangesNumber = tableCommit();
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return commitedChangesNumber;
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
    public int size() {
        int tableSize;
        try {
            tableSize = tableSize();
        } catch (IOException e) {
            throw new IllegalStateException("error: reading file error");
        }

        return tableSize;
    }
}
