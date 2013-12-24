package ru.fizteh.fivt.students.inaumov.filemap.base;

import ru.fizteh.fivt.students.inaumov.filemap.FileMapUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static ru.fizteh.fivt.students.inaumov.filemap.FileMapUtils.isEqual;

public abstract class AbstractDatabaseTable<Key, Value> implements AutoCloseable {
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public final Lock transactionLock = new ReentrantLock(true);
    public WeakHashMap<Key, Value> keyValueHashMap = new WeakHashMap<Key, Value>();

    public int size = 0;

    private class LazyHashMap {
        private HashMap<Key, Value> keyValueModifiedWeakHashMap = new HashMap<Key, Value>();

        Value lazyGet(Key key) throws IOException {
            if (keyValueModifiedWeakHashMap.containsKey(key)) {
                return keyValueModifiedWeakHashMap.get(key);
            }

            if (keyValueHashMap.containsKey(key)) {
                return keyValueHashMap.get(key);
            }

            loadTableLazy(key);

            return keyValueHashMap.get(key);
        }

        HashMap<Key,Value> getTable() {
            return keyValueModifiedWeakHashMap;
        }
    }

    protected class Diff {
        private LazyHashMap lazyHashMap = new LazyHashMap();

        private int unsavedChangesNumber = 0;

        public void change(Key key, Value value) {
            lazyHashMap.getTable().put(key, value);
        }

        public int commitChanges() throws IOException {
            int recordsChangedCount = 0;

            for (final Key key: lazyHashMap.getTable().keySet()) {
                Value newValue = lazyHashMap.lazyGet(key);

                if (!isEqual(keyValueHashMap.get(key), newValue)) {
                    if (newValue == null) {
                        keyValueHashMap.remove(key);
                    } else {
                        keyValueHashMap.put(key, (Value) newValue);
                    }

                    recordsChangedCount += 1;
                }
            }

            return recordsChangedCount;
        }

        public int getChangesCount() throws IOException {
            int recordsChangedCount = 0;

            for (final Key key: lazyHashMap.getTable().keySet()) {
                Value newValue = lazyHashMap.lazyGet(key);
                if (!isEqual(keyValueHashMap.get(key), newValue)) {
                    recordsChangedCount += 1;
                }
            }

            return recordsChangedCount;
        }

        public int calcSize() throws IOException {
            int tableSize = 0;

            for (final Key key: lazyHashMap.getTable().keySet()) {
                Value newValue = lazyHashMap.lazyGet(key);
                Value oldValue = keyValueHashMap.get(key);
                if (newValue == null && oldValue != null) {
                    tableSize -= 1;
                }
                if (newValue != null && oldValue == null) {
                    tableSize += 1;
                }
            }

            return tableSize;
        }

        public Value getValue(Key key) throws IOException {
            return lazyHashMap.lazyGet(key);
        }

        public int getSize() throws IOException {
            return calcSize();
        }

        public void incUnsavedChangesNumber() {
            unsavedChangesNumber += 1;
        }

        public int getUnsavedChangesNumber() {
            return unsavedChangesNumber;
        }

        public void clear() {
            lazyHashMap.getTable().clear();
            unsavedChangesNumber = 0;
        }
    }

    public final ThreadLocal<Diff> diff = new ThreadLocal<Diff>() {
        @Override
        public Diff initialValue() {
            return new Diff();
        }
    };

    protected TableState tableState;
    private final String tableName;
    private final String tableDir;

    protected abstract void loadTable() throws IOException;

    protected abstract void saveTable() throws IOException;

    protected abstract void loadTableLazy(Key key) throws IOException;

    public AbstractDatabaseTable(String tableDir, String tableName) {
        if (FileMapUtils.isStringNullOrEmpty(tableDir)) {
            throw new IllegalArgumentException("error: selected directory is null (or empty)");
        }
        if (FileMapUtils.isStringNullOrEmpty(tableName)) {
            throw new IllegalArgumentException("error: selected database name is null (or empty)");
        }

        this.tableState = TableState.NOT_INIT;
        this.tableName = tableName;
        this.tableDir = tableDir;

        try {
            loadTable();
        } catch (IOException e) {
            throw new IllegalArgumentException("error: can't load table, incorrect file format");
        }

        this.tableState = TableState.WORKING;
    }

    public String getName() {
        tableState.checkAvailable();

        return tableName;
    }

    public String getDir() {
        return tableDir;
    }

    public Value tableGet(Key key) throws IOException {
        tableState.checkAvailable();

        if (key == null) {
            throw new IllegalArgumentException("error: selected key is null");
        }

        try {
            transactionLock.lock();
            return diff.get().getValue(key);
        } finally {
            transactionLock.unlock();
        }

    }

    public Value tablePut(Key key, Value value) throws IOException {
        tableState.checkAvailable();

        if (key == null) {
            throw new IllegalArgumentException("error: selected key is null");
        }
        if (value == null) {
            throw new IllegalArgumentException("error: selected value is null");
        }

        try {
            transactionLock.lock();
            Value oldValue = diff.get().getValue(key);

            diff.get().change(key, value);
            if (!isEqual(value, oldValue)) {
                diff.get().incUnsavedChangesNumber();
            }

            return oldValue;
        } finally {
            transactionLock.unlock();
        }
    }

    public Value tableRemove(Key key) throws IOException {
        tableState.checkAvailable();

        if (key == null) {
            throw new IllegalArgumentException("error: selected key is null");
        }
        if (tableGet(key) == null) {
            return null;
        }

        try {
            transactionLock.lock();
            Value oldValue = diff.get().getValue(key);
            diff.get().change(key, null);
            diff.get().incUnsavedChangesNumber();

            return oldValue;
        } finally {
            transactionLock.unlock();
        }
    }

    public int tableCommit() throws IOException {
        tableState.checkAvailable();

        try {
            transactionLock.lock();
            size += diff.get().getSize();
            int committedChangesNumber = diff.get().commitChanges();
            diff.get().clear();

            try {
                saveTable();
            } catch (IOException e) {
                System.err.println("error: can't save table: " + e.getMessage());
            }

            return committedChangesNumber;
        } finally {
            transactionLock.unlock();
        }
    }

    public int tableRollback() throws IOException {
        tableState.checkAvailable();

        int rollbackedChangesCount = diff.get().getChangesCount();
        diff.get().clear();

        return rollbackedChangesCount;
    }

    public int tableSize() throws IOException {
        tableState.checkAvailable();

        return size + diff.get().getSize();
    }

    public int getUnsavedChangesNumber() {
        return diff.get().getUnsavedChangesNumber();
    }

    public void rawPut(Key key, Value value) {
        keyValueHashMap.put(key, value);
    }

    public Value rawGet(Key key) {
        return keyValueHashMap.get(key);
    }

    public String rawGetTableName() {
        return tableName;
    }

    public boolean isClosed() {
        return tableState.equals(TableState.CLOSED);
    }

    @Override
    public void close() throws Exception {
        if (tableState.equals(TableState.CLOSED)) {
            return;
        }

        tableCommit();
        tableState = TableState.CLOSED;
    }
}
