package io.korona.core.exception;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class OverLimitException extends RuntimeException{

    public OverLimitException(String s){
        super(s);
    }

}
