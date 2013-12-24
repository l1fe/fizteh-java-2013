package ru.fizteh.fivt.students.inaumov.storeable;

import ru.fizteh.fivt.storage.structured.*;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.ReadHandler;
import ru.fizteh.fivt.students.inaumov.shell.base.Shell;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class StoreableUtils {
    public static List<Object> parseValues(List<String> valuesTypeNames, Table table) throws ColumnFormatException {
        List<Object> result = new ArrayList<Object>(valuesTypeNames.size() - 1);

        for (int i = 1; i < valuesTypeNames.size(); ++i) {
            Object value = TypesFormatter.parseByClass(valuesTypeNames.get(i), table.getColumnType(i - 1));
            result.add(value);
        }

        return result;
    }

    public static String valuesTypeNamesToString(List<?> list, boolean nameNulls, String delimiter) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean firstEntry = true;

        for (final Object listEntry: list) {
            if (!firstEntry) {
                stringBuilder.append(delimiter);
            }
            firstEntry = false;

            if (listEntry == null) {
                if (nameNulls) {
                    stringBuilder.append("null");
                }
            } else {
                stringBuilder.append(listEntry.toString());
            }
        }

        return stringBuilder.toString();
    }

    public static TableInfo parseCreateCommand(String arguments) {
        TableInfo tableInfo = null;

        String tableName = arguments.split("\\s+")[0];
        arguments = arguments.replaceAll("\\s+", " ");

        int spaceFirstEntryIndex = arguments.indexOf(' ');
        if (spaceFirstEntryIndex == -1) {
            throw new IllegalArgumentException("error: select column value types");
        }

        String columnTypesString = arguments.substring(spaceFirstEntryIndex).replaceAll("\\((.*)\\)", "$1");

        String[] columnTypesNames = Shell.parseCommandParameters(columnTypesString);

        tableInfo = new TableInfo(tableName);

        for (int i = 0; i < columnTypesNames.length; ++i) {
            tableInfo.addColumn(TypesFormatter.getTypeByName(columnTypesNames[i]));
        }

        return tableInfo;
    }

    public static List<String> getColumnTypesNames(List<Class<?>> columnTypes) {
        List<String> columnTypesNames = new ArrayList<String>();
        for (final Class<?> columnType: columnTypes) {
            columnTypesNames.add(TypesFormatter.getSimpleName(columnType));
        }

        return columnTypesNames;
    }

    public static boolean isStringIncorrect(String string) {
        return string.matches("\\s*") || string.split("\\s+").length != 1;
    }

    public static void isValueCorrect(Object value, Class<?> type) throws ParseException {
        if (value == null) {
            return;
        }

        if (TypesFormatter.getSimpleName(type).equals("String")) {
            String stringValue = (String) value;
            stringValue = stringValue.trim();

            if (stringValue.isEmpty()) {
                return;
            }
            if (isStringIncorrect(stringValue)) {
                throw new ParseException("{" + stringValue + "}", 0);
            }
        }
    }

    public static List<String> formatColumnTypes(List<Class<?>> columnTypes) {
        List<String> formattedColumnTypes = new ArrayList<String>();
        for (final Class<?> columnType : columnTypes) {
            formattedColumnTypes.add(TypesFormatter.getSimpleName(columnType));
        }

        return formattedColumnTypes;
    }

    public static void writeTypesSignature(File signatureFile, List<Class<?>> columnTypes) throws IOException {
        File parent = signatureFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdir();
        }

        signatureFile.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(signatureFile));
        List<String> formattedColumnTypes = StoreableUtils.formatColumnTypes(columnTypes);

        String signature = StoreableUtils.valuesTypeNamesToString(formattedColumnTypes, true, " ");
        writer.write(signature);
        writer.close();
    }

    public static void writeSizeSignature(File signatureFile, int size) throws IOException {
        File parent = signatureFile.getParentFile();
        if (!parent.exists()) {
            parent.mkdir();
        }

        signatureFile.createNewFile();
        RandomAccessFile writer = new RandomAccessFile(signatureFile.getAbsolutePath(), "rw");
        writer.writeInt(size);
        writer.close();
    }

    public static int getSizeFromSizeSignature(File signatureFile) throws IOException {
        if (!signatureFile.exists()) {
            return -1;
        }

        ReadHandler readHandler = new ReadHandler(signatureFile.getAbsolutePath());
        int tableSize = readHandler.readInteger();
        readHandler.close();

        return tableSize;
    }
}
