package com.wiku.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Csv<T>
{

    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final String NEWLINE = System.lineSeparator();

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

    /**
     * Quitely maps object to csv, returning an Optional containing the result, or Optional.empty() in case
     * a JsonProcessingException occured ( also triggers exceptionHandler.handleException when it happens).
     *
     * @param objectToWrite   - object to map to CSV line
     * @param exceptionHander - ExceptionHandler to invoke for each object which could not be processed
     * @return Optional containing the result
     */
    public Optional<String> mapToCsvQuietly( T objectToWrite, ExceptionHander exceptionHander )
    {
        try
        {
            return Optional.of(writer.writeValueAsString(objectToWrite));
        }
        catch( JsonProcessingException e )
        {
            exceptionHander.handleException(e);
            return Optional.empty();
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
     * @return java Stream of objects
     * @throws IOException when stream can not be opened (eg. file does not exist)
     */
    public Stream<T> readFileAsStream( String path ) throws IOException
    {
        return doReadFileAsStream(path, ( exception ) -> {
        });
    }

    public void writeStreamToFile( Stream<T> stream, String outputPath ) throws CsvException
    {
        try (OutputStream os = new FileOutputStream(outputPath);
                BufferedOutputStream bos = new BufferedOutputStream(os);
                PrintWriter writer = new PrintWriter(os))
        {
            List<Exception> exceptionsList = new ArrayList<>();

            stream.map(object -> mapToCsvQuietly(object, exceptionsList::add))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(writer::write);

            if( !exceptionsList.isEmpty() )
            {
                String message = getAllExceptionsMessagesInOne(exceptionsList);
                throw new CsvException(
                        "Failed to write lines due to following errors: " + message);
            }
        }
        catch( IOException e )
        {
            throw new CsvException(e);
        }
    }

    private String getAllExceptionsMessagesInOne( List<Exception> exceptionList )
    {
        StringBuilder exceptionMessageBuilder = new StringBuilder();
        exceptionList.stream()
                .map(Exception::toString)
                .map(exceptionMessage -> exceptionMessage + "," + NEWLINE)
                .forEach(exceptionMessageBuilder::append);
        return exceptionMessageBuilder.toString();
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
            return schema;
        }

    }
}
