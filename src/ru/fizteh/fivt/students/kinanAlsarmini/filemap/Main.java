package ru.fizteh.fivt.students.kinanAlsarmini.filemap;

import java.io.IOException;

class Main {
    public static void main(String[] args) throws IOException {
        FileMap filemap = new FileMap(System.getProperty("fizteh.db.dir"), "db.dir");

        if (args.length == 0) {
            filemap.startInteractive();
        } else {
            StringBuilder commands = new StringBuilder();
            for (int i = 0; i < args.length; i++) {
                commands.append(args[i]);
                if (i < args.length - 1) {
                    commands.append(" ");
                }
            }

            filemap.startBatch(commands.toString());
        }
    }
}
