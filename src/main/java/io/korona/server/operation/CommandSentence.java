package io.korona.server.operation;

import java.util.List;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class CommandSentence {

    private Operate operate;

    private int index = 0;

    private List<Token> tokenList;

    public CommandSentence(Operate operate){
        this.operate = operate;
    }

    public CommandSentence(List<Token> tokens){
        if(tokens.isEmpty()) throw new RuntimeException("unexpect command!");

        Token op = tokens.get(0);
        String ops = op.getValue();

        try {
            this.operate = Operate.valueOf(ops.toUpperCase());
        } catch (IllegalArgumentException e){
            System.out.println("Unknown support operate type of '"+ops.toUpperCase()+"'!");
        }

        this.tokenList = tokens;
        this.index = 1;

    }

    public Token readToken(){
        return this.tokenList.get(this.index++);
    }

    public boolean isEnd(){
        return this.index > this.tokenList.size() - 1;
    }

    public Operate getOperate() {
        return operate;
    }

    public static final class Token{

        private Object tokenValue;

        public Token(String token){
            if(!token.isEmpty()){
                if(token.length() > 1 && token.charAt(0) == '"' && token.charAt(token.length() - 1) == '"'){
                    var tk = token.substring(1);
                    this.tokenValue = tk.substring(0, tk.length() - 1);
                }else {
                    Object val = null;

                    if((val = parseInteger(token)) != null){
                        this.tokenValue = val;
                    }else if((val = parseLong(token)) != null){
                        this.tokenValue = val;
                    }else{
                        this.tokenValue = token;
                    }
                }
            }
        }

        private static Integer parseInteger(String token){
            try{
                return Integer.parseInt(token);
            }catch (NumberFormatException ignore){
                return null;
            }
        }

        private static Long parseLong(String token){
            try{
                return Long.parseLong(token);
            }catch (NumberFormatException ignore){
                return null;
            }
        }

        public <T> T getValue(){
            return (T) this.tokenValue;
        }

    }

}
