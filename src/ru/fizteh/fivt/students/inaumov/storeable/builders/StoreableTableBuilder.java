package ru.fizteh.fivt.students.inaumov.storeable.builders;

import ru.fizteh.fivt.storage.structured.ColumnFormatException;
import ru.fizteh.fivt.storage.structured.Storeable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.TableBuilder;
import ru.fizteh.fivt.students.inaumov.multifilemap.MultiFileMapUtils;
import ru.fizteh.fivt.students.inaumov.storeable.base.DatabaseTable;
import ru.fizteh.fivt.students.inaumov.storeable.base.DatabaseTableProvider;

import java.io.File;
import java.text.ParseException;
import java.util.Set;

public class StoreableTableBuilder implements TableBuilder {
    DatabaseTableProvider tableProvider;
    DatabaseTable table;

    private int currentBucket;
    private int currentFile;

    public StoreableTableBuilder(DatabaseTableProvider tableProvider, DatabaseTable table) {
        this.tableProvider = tableProvider;
        this.table = table;
    }

    @Override
    public String get(String key) {
        Storeable value = table.get(key);
        try {
            String representation = tableProvider.serialize(table, value);

            return representation;
        } catch (ColumnFormatException e) {
            return null;
        }
    }

    @Override
    public void put(String key, String value) {
        MultiFileMapUtils.checkKeyPlacement(key, currentBucket, currentFile);

        Storeable objectValue = null;

        try {
            objectValue = tableProvider.deserialize(table, value);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }

        table.rawPut(key, objectValue);
    }

    @Override
    public Set<String> getKeys() {
        return table.rawGetKeys();
    }

    @Override
    public File getTableDir() {
        return new File(table.getDir(), table.getName());
    }

    @Override
    public void setCurrentFile(File curFile) {
        currentBucket = MultiFileMapUtils.parseCurrentBucketNumber(curFile.getParentFile());
        currentFile = MultiFileMapUtils.parseCurrentFileNumber(curFile);
    }
}
