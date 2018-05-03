package com.wiku.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;

public class Csv<T>
{

    private final ObjectWriter writer;
    private final ObjectReader reader;

    private Csv( ObjectWriter writer, ObjectReader reader )
    {
        this.writer = writer;
        this.reader = reader;
    }

    public static <T> CsvBuilder from( Class<T> schemaClass )
    {
        return new CsvBuilder<T>(schemaClass);
    }

    public String mapToCsv( T objectToWrite ) throws CsvException
    {
        try
        {
            return writer.writeValueAsString(objectToWrite);
        }
        catch( JsonProcessingException e )
        {
            throw new CsvException(e);
        }
    }

    public T mapToObject( String csvLine ) throws CsvException
    {
        try
        {
            return reader.readValue(csvLine);
        }
        catch( IOException e )
        {
            throw new CsvException(e);
        }
    }


    public static class CsvBuilder<T>
    {

        private final Class<T> schemaClass;
        private char separator;
        private boolean newLine;

        public CsvBuilder( Class<T> schemaClass )
        {
            this.schemaClass = schemaClass;
        }

        public CsvBuilder<T> withSeparator( char separator )
        {
            this.separator = separator;
            return this;
        }

        public Csv build()
        {
            CsvMapper mapper = new CsvMapper();
            CsvSchema schema = createSchema(mapper);
            ObjectWriter writer = mapper.writer(schema);
            ObjectReader reader = mapper.readerFor(schemaClass).with(schema);
            return new Csv<>(writer, reader);
        }

        private CsvSchema createSchema( CsvMapper mapper )
        {
            CsvSchema schema = mapper.schemaFor(schemaClass).withColumnSeparator(separator);
            if( newLine )
            {
                schema = schema.withLineSeparator(System.lineSeparator());
            }
            return schema;
        }

        public CsvBuilder withNewline()
        {
            this.newLine = true;
            return this;
        }
    }
}
