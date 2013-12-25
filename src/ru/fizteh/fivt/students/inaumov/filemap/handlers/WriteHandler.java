package ru.fizteh.fivt.students.inaumov.filemap.handlers;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Set;

import ru.fizteh.fivt.students.inaumov.filemap.base.AbstractDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.TableBuilder;

public class WriteHandler implements Closeable {
    private RandomAccessFile outputFile = null;

    public WriteHandler(String fileName) throws IOException {
        try {
            outputFile = new RandomAccessFile(fileName, "rw");
        } catch (FileNotFoundException exception) {
            throw new IOException("can't create file " + fileName);
        }

        outputFile.setLength(0);
    }

    public static void saveToFile(String filePath, Set<String> keysToSave, TableBuilder builder) throws IOException {
        WriteHandler writer = new WriteHandler(filePath);

        for (final String key: keysToSave) {
            writer.writeEntry(key, builder.get(key));
        }

        try {
            writer.close();
        } catch (IOException e) {
            //
        }
    }

    public static void saveToFileFromHashMap(String filePath, HashMap<String, String> mapToSave) throws IOException {
        WriteHandler writer = new WriteHandler(filePath);

        for (final String key : mapToSave.keySet()) {
            writer.writeEntry(key, mapToSave.get(key));
        }

        writer.close();
    }

    public void writeEntry(String key, String value) throws IOException {
        outputFile.writeInt(key.getBytes(AbstractDatabaseTable.CHARSET).length);
        outputFile.writeInt(value.getBytes(AbstractDatabaseTable.CHARSET).length);

        outputFile.write(key.getBytes(AbstractDatabaseTable.CHARSET));
        outputFile.write(value.getBytes(AbstractDatabaseTable.CHARSET));
    }

    public void close() throws IOException {
        outputFile.close();
    }
}
