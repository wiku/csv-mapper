package com.wiku.csv;

public class CsvException extends Exception
{
    public CsvException( Exception e )
    {
        super(e);
    }

    public CsvException( String message )
    {
        super(message);
    }

    public CsvException( String s, Throwable throwable )
    {
        super(s, throwable);
    }
}
