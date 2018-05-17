package com.wiku.csv;

public class CsvLineParsingFailedException extends RuntimeException
{
    public CsvLineParsingFailedException( String s, Throwable throwable )
    {
        super(s, throwable);
    }
}
