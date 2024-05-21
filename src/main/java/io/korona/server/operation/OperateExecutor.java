package io.korona.server.operation;

import io.korona.core.data.serialize.TypeConvert;
import io.korona.server.KoronaDatabase;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.StringJoiner;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class OperateExecutor {

    private final KoronaDatabase database;

    public OperateExecutor(KoronaDatabase database){
        this.database = database;
    }

    public void execute(CommandSentence commandSentence){
        try{
            switch(commandSentence.getOperate()){
                case GET -> {
                    var a = this.database.get(commandSentence.readToken().getValue());
                    if(a == null){
                        System.out.println("(not found)");
                    }else{
                        System.out.println("("+a.getClass().getSimpleName()+") "+a);
                    }
                }
                case SET -> {
                    this.database.put(commandSentence.readToken().getValue(), commandSentence.readToken().getValue());

                }
                case COUNT -> {
                    System.out.println(this.database.keyCount());
                }
                case KEYS -> {
                    StringJoiner sb = new StringJoiner("\n");
                    var it = this.database.keys();
                    while(it.hasNext()){
                        var obj = TypeConvert.serialize(it.next().bytes());
                        assert obj != null;
                        sb.add("("+obj.getClass().getSimpleName()+") "+obj);
                    }

                    System.out.println(sb);
                }
                case DEL -> {
                    this.database.delete(commandSentence.readToken().getValue());
                }
                case STATUS -> {
                    var status = this.database.getRunningStatus();
                    StringBuilder builder = new StringBuilder();

                    var date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            .withZone(ZoneId.systemDefault())
                            .format(Instant.ofEpochMilli(status.startTime));
                    builder.append("Start Time: ").append(date).append("\n")
                            .append("Compact Remaining Time: ").append(status.compactRemaining).append("\n")
                            .append("DataSync Remaining Time: ").append(status.dataSyncRemaining).append("\n")
                            .append("KeydirDump Remaining Time: ").append(status.keydirDumpRemaining).append("\n")
                            .append("Buffer Status: ").append(status.bufferMaxSize - status.bufferRemaining).append("/").append(status.bufferMaxSize);

                    System.out.println(builder);

                }

            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

}
