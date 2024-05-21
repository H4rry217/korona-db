package io.korona.core.data.serialize;

import io.korona.core.exception.NotSupportTypeException;
import io.korona.core.typeserialize.StringTypeSerializer;
import io.korona.core.typeserialize.number.IntTypeSerializer;
import io.korona.core.typeserialize.number.LongTypeSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: korona-db
 * @description:
 * @author: H4rry217
 **/

public class TypeConvert {

    private final static Map<Class, TypeSerializer> TYPE_SERIALIZER_MAP = new HashMap<>();
    private final static Map<Byte, TypeSerializer> TYPE_SERIALIZER_MAP_ = new HashMap<>();

    static {
        registerType(Integer.class, new IntTypeSerializer());
        registerType(Long.class, new LongTypeSerializer());
        registerType(String.class, new StringTypeSerializer());
    }

    public static void registerType(Class typeClazz, TypeSerializer serializer){
        TYPE_SERIALIZER_MAP.put(typeClazz, serializer);
        TYPE_SERIALIZER_MAP_.put(serializer.getType().getType(), serializer);
    }

    public static byte[] deserialize(Object object){
        var clazz = object.getClass();
        if(TYPE_SERIALIZER_MAP.containsKey(clazz)){
            return TYPE_SERIALIZER_MAP.get(clazz).deserialize(object);
        }else{
            throw new NotSupportTypeException("Cannot find typeSerializer to deserialize type "+clazz);
        }
    }

    public static Object serialize(byte[] bytes){
        if(bytes.length > 0){
            byte type = bytes[0];
            if(TYPE_SERIALIZER_MAP_.containsKey(type)){
                return TYPE_SERIALIZER_MAP_.get(type).serialize(bytes);
            }else{
                throw new NotSupportTypeException("Cannot find typeSerializer to serialize type "+type);
            }
        }else{
            return null;
        }
    }

}
