package com.wrmsr.presto.codec;

/*
byte[] -> encoded<foo>('json')
selcct decode(cast(some_bytes as encoded<int>('json')))
well not necessarily byte[] its whatever typeCodec.getFromType() is
*/
public class EncodedCast
{
}
