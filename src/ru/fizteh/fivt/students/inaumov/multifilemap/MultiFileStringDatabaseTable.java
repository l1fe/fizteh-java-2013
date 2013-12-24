package ru.fizteh.fivt.students.inaumov.multifilemap;

import ru.fizteh.fivt.students.inaumov.filemap.base.StringDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.SimpleTableBuilder;
import ru.fizteh.fivt.students.inaumov.multifilemap.handlers.*;

import java.io.File;
import java.io.IOException;

public class MultiFileStringDatabaseTable extends StringDatabaseTable {
    public MultiFileStringDatabaseTable(String dir, String tableName) {
        super(dir, tableName);
    }

    protected void saveTable() throws IOException {
        SaveHandler.saveTable(new SimpleTableBuilder(this));
    }

    protected void loadTable() throws IOException {
        LoadHandler.loadTable(new SimpleTableBuilder(this));
    }

    protected void loadTableLazy(String key) throws IOException {
        // empty
    }

    private File getTableDir() {
        File tableDir = new File(getDir(), getName());
        if (!tableDir.exists()) {
            tableDir.mkdir();
        }

        return tableDir;
    }
}
