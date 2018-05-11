package com.wiku.csv;

import com.fasterxml.jackson.core.JsonProcessingException;

public class CsvException extends Exception
{
    public CsvException( Exception e )
    {
        super(e);
    }

    public CsvException(String message)
    {
        super(message);
    }
}
