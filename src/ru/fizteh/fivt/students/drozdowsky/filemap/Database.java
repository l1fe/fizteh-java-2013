package ru.fizteh.fivt.students.drozdowsky.filemap;

import com.sun.tools.javac.util.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

public class Database {
    private File dbPath;
    private HashMap<String, String> db;

    static final int BUFFSIZE = 100000;

    public Database(File dbPath) throws IOException {
        this.dbPath = dbPath;
        db = new HashMap<String, String>();
        validityCheck();
        if (dbPath.exists()) {
            readDB();
        }
    }

    private void validityCheck() throws IOException {
        if (!dbPath.getParentFile().exists()) {
            fatalError(dbPath.getParentFile().getAbsolutePath() + ": No such file or directory");
        } else if (dbPath.exists() && !dbPath.isFile()) {
            fatalError(dbPath.getAbsolutePath() + ": Not a file");
        }
    }

    public void close() {
        writeDB();
    }

    private void fatalError(String error) throws IOException {
        throw new IOException(error);
    }

    private void error(String aError) {
        System.err.println(aError);
    }

    public boolean get(String[] args) {
        if (args.length != 2) {
            error("usage: get key");
            return false;
        }
        if (db.get(args[1]) == null) {
            System.out.println("not found");
        } else {
            System.out.println("found\n" + db.get(args[1]));
        }
        return true;
    }

    public boolean put(String[] args) {
        if (args.length != 3) {
            error("usage: put key value");
            return false;
        }
        if (db.get(args[1]) == null) {
            System.out.println("new");
        } else {
            System.out.println("overwrite\n" + db.get(args[1]));
        }
        db.put(args[1], args[2]);
        return true;
    }

    public boolean remove(String[] args) {
        if (args.length != 2) {
            error("usage: remove key");
            return false;
        }

        if (db.get(args[1]) == null) {
            System.out.println("not found");
        } else {
            db.remove(args[1]);
            System.out.println("removed");
        }
        return true;
    }

    private void readDB() throws IOException {
        try {
            ArrayList<Pair<String, Integer>> offset = new ArrayList<Pair<String, Integer>>();
            FileInputStream inputDB = new FileInputStream(dbPath);

            Byte next;
            int byteRead = 0;
            ByteBuffer key = ByteBuffer.allocate(BUFFSIZE);

            while ((next = (byte) inputDB.read()) != -1) {
                if (next == 0) {
                    byte[] sizeBuf = new byte[4];
                    for (int i = 0; i < 4; i++) {
                        if ((sizeBuf[i] = (byte) inputDB.read()) == -1) {
                            fatalError("Incorrect database");
                        }
                    }
                    int valueSize = ByteBuffer.wrap(sizeBuf).getInt();

                    byte[] keyArray = new byte[key.position()];
                    byteRead += key.position() + 4 + 1;
                    key.clear();
                    key.get(keyArray);
                    offset.add(new Pair<String, Integer>(new String(keyArray, "UTF-8"), valueSize));
                    key.clear();

                } else {
                    key.put(next);
                }
            }

            if ((offset.size() == 0 && key.position() != 0) || (offset.get(0).snd != byteRead)) {
                fatalError("Incorrect database");
            }

            if (offset.get(0).snd - byteRead != 0) {
                fatalError("Incorrect database");
            }
            for (int i = 0; i < offset.size() - 1; i++) {
                offset.set(i, new Pair<String, Integer>(offset.get(i).fst, offset.get(i + 1).snd - byteRead));
            }
            int n = offset.size();
            offset.set(n - 1, new Pair<String, Integer>(offset.get(n - 1).fst, key.position()));

            int prevOffset = 0;
            for (Pair<String, Integer> now: offset) {
                Integer currentOffset = now.snd;
                if (currentOffset <= prevOffset) {
                    fatalError("Incorrect database");
                } else {
                    ByteBuffer valueBuf = ByteBuffer.allocate(currentOffset - prevOffset);
                    for (int i = prevOffset; i < currentOffset; i++) {
                        valueBuf.put(key.get(i));
                    }
                    prevOffset = currentOffset;
                    String value = new String(valueBuf.array(), "UTF-8");
                    db.put(now.fst, value);
                }
            }
        } catch (FileNotFoundException e) {
            fatalError(dbPath.getAbsolutePath() + ": No such file or directory");
        }
    }

    private void writeDB() {
        try {
            FileOutputStream out = new FileOutputStream(dbPath);
            ArrayList<Integer> length = new ArrayList<Integer>();
            ArrayList<String> values = new ArrayList<String>();
            ArrayList<String> keys = new ArrayList<String>();
            int totalLength = 0;
            for (String key:db.keySet()) {
                keys.add(key);
                values.add(db.get(key));
                totalLength += key.getBytes("UTF-8").length;
                length.add(db.get(key).getBytes("UTF-8").length);
                totalLength += 4;
                totalLength += "\0".getBytes("UTF-8").length;
            }
            for (int i = 0; i < keys.size(); i++) {
                out.write(keys.get(i).getBytes("UTF-8"));
                out.write("\0".getBytes("UTF-8"));
                out.write(ByteBuffer.allocate(4).putInt(totalLength).array());
                totalLength += length.get(i);
            }
            for (int i = 0; i < keys.size(); i++) {
                out.write(values.get(i).getBytes("UTF-8"));
            }
        } catch (IOException e) {
            error(dbPath.getAbsolutePath() + e.toString());
        }
    }

}
