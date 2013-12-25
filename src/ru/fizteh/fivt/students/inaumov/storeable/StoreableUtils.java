package ru.fizteh.fivt.students.inaumov.storeable;

import ru.fizteh.fivt.storage.structured.*;
import ru.fizteh.fivt.students.inaumov.filemap.base.AbstractDatabaseTable;
import ru.fizteh.fivt.students.inaumov.filemap.builders.TableBuilder;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.ReadHandler;
import ru.fizteh.fivt.students.inaumov.filemap.handlers.WriteHandler;
import ru.fizteh.fivt.students.inaumov.multifilemap.MultiFileMapUtils;
import ru.fizteh.fivt.students.inaumov.multifilemap.handlers.SaveHandler;
import ru.fizteh.fivt.students.inaumov.shell.base.Shell;
import ru.fizteh.fivt.students.inaumov.storeable.base.DatabaseTable;
import ru.fizteh.fivt.students.inaumov.storeable.builders.StoreableTableBuilder;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static ru.fizteh.fivt.students.inaumov.filemap.FileMapUtils.isEqual;

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

        //RandomAccessFile writer = new RandomAccessFile(signatureFile.getAbsolutePath(), "rw");
        PrintWriter writer = new PrintWriter(signatureFile);
        writer.write(Integer.toString(size));
        //writer.writeInt(size);
        writer.close();
    }

    public static int getSizeFromSizeSignature(File signatureFile) throws IOException {
        if (!signatureFile.exists()) {
            return -1;
        }

        int tableSize = 0;

        Scanner scanner = new Scanner(signatureFile);
        if (scanner.hasNextInt()) {
            tableSize = scanner.nextInt();
        }
        scanner.close();

        return tableSize;
    }

    public static int countRecords(String tableDirString) throws IOException {
        File tableDir = new File(tableDirString);
        if (!tableDir.exists()) {
            return 0;
        }

        int recordsNum = 0;

        for (final File bucket : tableDir.listFiles()) {
            if (bucket.isDirectory()) {
                for (final File file : bucket.listFiles()) {
                    recordsNum += ReadHandler.getRecordsNumberFromFile(file.getAbsolutePath());
                }
            }
        }

        return recordsNum;
    }

    public static void changeFileData(DatabaseTable table, HashMap<String, String> mapFile) {
        Storeable newValue;

        for (final String key : table.diff.get().lazyHashMap.keyValueModifiedHashMap.keySet()) {
            newValue = table.diff.get().lazyHashMap.keyValueModifiedHashMap.get(key);
            String newValueString = table.tableProvider.serialize(table, newValue);

            if (mapFile.containsKey(key)) {
                if (!isEqual(newValueString, mapFile.get(key))) {
                    if (table.diff.get().lazyHashMap.keyValueModifiedHashMap.get(key) == null) {
                        mapFile.remove(key);
                    } else {
                        mapFile.put(key, newValueString);
                    }
                }
            }
        }
    }

    public static void saveDatabaseTableAfterCommit(StoreableTableBuilder builder) throws IOException {
        File tableDir = builder.getTableDir();
        ArrayList<Set<String>> keysToSave = new ArrayList<Set<String>>();
        boolean bucketIsEmpty;

        for (int bucketNumber = 0; bucketNumber < SaveHandler.BUCKET_NUM; ++bucketNumber) {
            keysToSave.clear();
            for (int i = 0; i < SaveHandler.TABLES_IN_ONE_DIR; ++i) {
                keysToSave.add(new HashSet<String>());
            }
            bucketIsEmpty = true;

            for (final String key: builder.getKeys()) {
                if (MultiFileMapUtils.getDirNumber(key) == bucketNumber) {
                    int fileNumber = MultiFileMapUtils.getFileNumber(key);
                    keysToSave.get(fileNumber).add(key);
                    bucketIsEmpty = false;
                }
            }

            String bucketName = bucketNumber + ".dir";
            File bucketDirectory = new File(tableDir, bucketName);

            if (bucketDirectory.exists()) {
                for (final File fileEntry : bucketDirectory.listFiles()) {
                    if (fileEntry.isDirectory()) {
                        continue;
                    }
                    if (fileEntry.length() > 0) {
                        bucketIsEmpty = false;
                        break;
                    }
                }
            }

            if (bucketIsEmpty) {
                MultiFileMapUtils.deleteFile(bucketDirectory);
            }

            for (int fileN = 0; fileN < SaveHandler.TABLES_IN_ONE_DIR; ++fileN) {
                String fileName = fileN + ".dat";
                File file = new File(bucketDirectory, fileName);

                if (keysToSave.get(fileN).isEmpty() && (!file.exists() || file.length() == 0)) {
                    MultiFileMapUtils.deleteFile(file);
                    continue;
                }

                if (!bucketDirectory.exists()) {
                    bucketDirectory.mkdir();
                }

                if (!keysToSave.get(fileN).isEmpty()) {
                    HashMap<String, String> mapFile = ReadHandler.loadFileIntoMap(file.getAbsolutePath());
                    changeFileData(builder.table, mapFile);
                    WriteHandler.saveToFileFromHashMap(file.getAbsolutePath(), mapFile);
                }
            }
        }
    }
}
