package ru.fizteh.fivt.students.nadezhdakaratsapova.shell;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CommandsController {
    private Map<String, Command> commandsStorage = new HashMap<String, Command>();

    public void addCmd(Command cmd) {
        commandsStorage.put(cmd.getName(), cmd);
    }

    public void runCommand(CurrentDirectory currentDirectory, String[] command) throws IOException {
        if (!(command[0].length() == 0)) {
            Command cmd = commandsStorage.get(command[0]);
            if (cmd == null) {
                throw new IOException(command[0] + ": unknown command");
            } else {
                if ((command.length - 1) != cmd.getArgsCount()) {
                    throw new IOException(cmd.getName() + ": wrong number of arguments. It should be " + cmd.getArgsCount());
                } else {
                    cmd.execute(currentDirectory, command);
                }
            }
        }

    }


}