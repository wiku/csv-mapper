package com.wiku.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

public class Csv<T>
{

    /**
     * Internal interface used to pass exceptions from streams
     */
    public interface ExceptionHander
    {
        void handleException( Exception e );
    }

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

    /**
     * Quiet version which doesn't throw exceptions, but passes them to the provided ExceptionHandler.
     * Parses a single CSV line to POJO and returns optional containing it.
     * In case parsing failed, Optional.empty and calls the provided exceptionHandler
     *
     * @param csvLine         - single csv line to parse
     * @param exceptionHander - called when any kind of IOException is thrown during parsing
     * @return Optional containing the object read from csv line or Optional.empty() in case parsing error occured
     */
    public Optional<T> mapToObjectQuietly( String csvLine, ExceptionHander exceptionHander )
    {
        try
        {
            return Optional.of(reader.readValue(csvLine));
        }
        catch( IOException e )
        {
            exceptionHander.handleException(e);
            return Optional.empty();
        }
    }

    /**
     * Creates a Stream of POJOs from CSV file. Exception handler can be provided
     * to handle any kinds of exceptions that might occur while reading/parsing
     *
     * @param path            - path to csv file
     * @param exceptionHander - it's handleException method will be called
     *                        each time a line cannot be parsed, or if file reading fails for some reason
     * @return java Stream of objects
     */
    public Stream<T> readFileAsStream( String path, ExceptionHander exceptionHander )
    {
        try
        {
            return doReadFileAsStream(path, exceptionHander);
        }
        catch( IOException e )
        {
            exceptionHander.handleException(e);
            return Stream.empty();
        }
    }

    /**
     * Creates a Stream of POJOs from CSV file. Swallows any kind of mapping exceptions and omits faulty lines.
     *
     * @param path - path to csv file
     * @throws IOException when stream can not be opened (eg. file does not exist)
     * @return java Stream of objects
     */
    public Stream<T> readFileAsStream( String path ) throws IOException
    {
        return doReadFileAsStream(path, ( exception ) -> { });
    }

    private Stream<T> doReadFileAsStream( String path, ExceptionHander exceptionHander ) throws IOException
    {
        return Files.lines(Paths.get(path))
                .map(line -> mapToObjectQuietly(line, exceptionHander))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }


    public static class CsvBuilder<T>
    {

        private final Class<T> schemaClass;
        private char separator = ',';
        private boolean newLine = false;

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
