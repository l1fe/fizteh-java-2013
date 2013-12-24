package ru.fizteh.fivt.students.inaumov.filemap;

import ru.fizteh.fivt.students.inaumov.filemap.base.StringDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.SimpleTableBuilder;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.*;

import java.io.File;
import java.io.IOException;

public class SingleFileStringDatabaseTable extends StringDatabaseTable {
    private static final String DEFAULT_DATABASE_FILE_NAME = "db.dat";

    public SingleFileStringDatabaseTable(String dir, String tableName) {
        super(dir, tableName);
    }

    protected void loadTable() throws IOException {
        ReadHandler.loadFromFile(getDatabaseFilePath(), new SimpleTableBuilder(this));
    }

    protected void loadTableLazy(String key) throws IOException {
        //empty
    }

    protected void saveTable() throws IOException {
        WriteHandler.saveToFile(getDatabaseFilePath(), keyValueHashMap.keySet(), new SimpleTableBuilder(this));
    }

    private String getDatabaseFilePath() {
        return new File(getDir(), DEFAULT_DATABASE_FILE_NAME).getAbsolutePath();
    }
}
