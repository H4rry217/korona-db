package io.korona.server.cli;

import io.korona.server.operation.CommandSentence;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class CommandParser {

    public CommandParser(){

    }

    public List<CommandSentence.Token> resolveToken(String command){
        List<String> stringTokens = new ArrayList<>();
        StringBuilder currentArgument = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;

        for (char c : command.toCharArray()) {
            if (c == ' ' && !inQuotes) {
                if (!currentArgument.isEmpty()) {
                    stringTokens.add(currentArgument.toString());
                    currentArgument.setLength(0);
                }
            } else if (c == '"') {
                if (!escaped) {
                    inQuotes = !inQuotes;
                }
                currentArgument.append(c);
            } else if (c == '\\' && inQuotes && !escaped) {
                escaped = true;
            } else {
                currentArgument.append(c);
                escaped = false;
            }
        }

        if (!currentArgument.isEmpty()) {
            stringTokens.add(currentArgument.toString());
        }

        return stringTokens.stream().map(CommandSentence.Token::new).toList();
    }

}
