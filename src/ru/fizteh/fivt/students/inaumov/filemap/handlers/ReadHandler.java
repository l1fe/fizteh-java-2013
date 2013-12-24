package ru.fizteh.fivt.students.inaumov.filemap.handlers;

import java.io.*;

import ru.fizteh.fivt.storage.structured.Table;
import ru.fizteh.fivt.students.inaumov.filemap.base.AbstractDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.TableBuilder;

public class ReadHandler implements Closeable {
    private RandomAccessFile inputFile = null;
    private String fileName = null;

    public ReadHandler(String fileName) throws IOException {
        try {
            inputFile = new RandomAccessFile(fileName, "r");
        } catch (FileNotFoundException exception) {
            //
        }

        if (inputFile.length() == 0) {
            throw new IllegalArgumentException("error: empty file: " + fileName);
        }
        this.fileName = fileName;
    }

    public static void loadFromFile(String filePath, TableBuilder builder) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            return;
        }

        ReadHandler reader = new ReadHandler(filePath);

        while (!reader.readEnd()) {
            int keyLength = reader.readInteger();
            int valueLength = reader.readInteger();

            String key = reader.readString(keyLength);
            String value = reader.readString(valueLength);

            builder.put(key, value);
        }

        reader.close();
    }

    public static int getRecordsNumberFromFile(String filePath) throws IOException {
        int recordsNumber = 0;

        File file = new File(filePath);
        if (!file.exists()) {
            return recordsNumber;
        }

        ReadHandler reader = new ReadHandler(filePath);

        while (!reader.readEnd()) {
            int keyLength = reader.readInteger();
            int valueLength = reader.readInteger();

            String key = reader.readString(keyLength);
            String value = reader.readString(valueLength);

            recordsNumber += 1;
        }

        reader.close();

        return recordsNumber;
    }

    public int readInteger() throws IOException {
        int result = inputFile.readInt();
        return result;
    }

    public String readString(int stringLength) throws IOException {
        byte[] stringBytes = null;

        if (stringLength <= 0) {
            throw new IllegalStateException("Some key or value length is negative in " + fileName);
        }
        try {
            stringBytes = new byte[stringLength];
        } catch (OutOfMemoryError error) {
            throw new IllegalStateException("Some key or value length is too long in " + fileName);
        }

        inputFile.read(stringBytes);
        return new String(stringBytes, AbstractDatabaseTable.CHARSET);
    }

    public boolean readEnd() throws IOException {
        if (inputFile == null) {
            return true;
        }

        if (inputFile.getFilePointer() < inputFile.length() - 1) {
            return false;
        }

        return true;
    }

    public void close() throws IOException {
        inputFile.close();
    }
}
