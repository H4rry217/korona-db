package io.korona.server.cli;

import io.korona.server.operation.CommandSentence;
import io.korona.server.operation.Operate;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class CommandLineConsole {

    public static final String PROMPT = "korona> ";

    private final LineReader lineReader;

    private final CommandParser commandParser;

    public CommandLineConsole() throws IOException {
        Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();

        this.lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();

        this.commandParser = new CommandParser();

        this.outputInformation();
    }

    public void outputInformation(){
        String banner = "";
        try(InputStream is = this.getClass().getClassLoader().getResourceAsStream("banner")){
            assert is != null;
            banner = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println(banner);
    }

    public CommandSentence inputRead() throws IOException {
        String line;
        try {
            line = this.lineReader.readLine(PROMPT);

            List<CommandSentence.Token> tokens = this.commandParser.resolveToken(line);

            return new CommandSentence(tokens);
        } catch (UserInterruptException | EndOfFileException ignore) {
            return new CommandSentence(Operate.EXIT);
        }
    }


}
