package ru.fizteh.fivt.students.inaumov.storeable;

import ru.fizteh.fivt.storage.structured.*;

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

    public static String valuesTypeNamesToString(List<?> list) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean firstEntry = true;

        for (final Object listEntry: list) {
            if (!firstEntry) {
                stringBuilder.append(" ");
            }
            firstEntry = false;

            if (listEntry == null) {
                stringBuilder.append("null");
            } else {
                stringBuilder.append(listEntry.toString());
            }
        }

        return stringBuilder.toString();
    }

    public static TableInfo parseCreateCommand(String arguments) {
        TableInfo tableInfo = null;
        String[] columnTypesNames = arguments.split(" ");
        if (columnTypesNames.length <= 1) {
            throw new IllegalArgumentException("error: can't parse arguments");
        }

        String tableName = columnTypesNames[0];
        tableInfo = new TableInfo(tableName);

        for (int i = 1; i < columnTypesNames.length; ++i) {
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
        return string.matches("\\s+") || string.split("\\s+").length != 1;
    }

    public static void isValueCorrect(Object value, Class<?> type) throws ParseException {
        if (value == null) {
            return;
        }

        if (TypesFormatter.getSimpleName(type).equals("String")) {
            String stringValue = (String) value;
            if (isStringIncorrect(stringValue)) {
                throw new ParseException("", 0);
            }
        }
    }
}