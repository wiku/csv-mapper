package com.wiku.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;

public class Csv<T>
{
    private final ObjectWriter writer;
    private final ObjectReader reader;
    private final String header;
    private final boolean skipEmptyLines;

    private Csv( ObjectWriter writer, ObjectReader reader, String header, boolean skipEmptyLines )
    {
        this.writer = writer;
        this.reader = reader;
        this.header = header;
        this.skipEmptyLines = skipEmptyLines;
    }

    public static <T> CsvBuilder from( Class<T> schemaClass )
    {
        return new CsvBuilder<T>(schemaClass);
    }

    /**
     * Maps a single object to CSV line
     * @param objectToWrite object to write
     * @return a single line of CSV representing the object
     * @throws CsvException when a json exception occurs during parsing
     */
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
     * a JsonProcessingException occurred ( also triggers exceptionHandler.handleException when it happens).
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

    /**
     * Maps a single CSV line to Object
     *
     * @param csvLine - line in CSV format
     * @return object created from the line
     * @throws CsvException when IO exception or mapping error occurs
     */
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
     * Writes stream of objects to CSV file line by line, starting with header (in case withHeader option is set).
     * Any kind of writing or parsing exception stops writing and is rethrown as CsvException.
     *
     * @param stream     - stream of objects to write
     * @param outputPath - output file to be written (overrides the file if exists)
     * @throws CsvException - if either the whole file, or a single object cannot be written or mapped to CSV.
     */
    public void writeStreamToFile( Stream<T> stream, String outputPath ) throws CsvException
    {
        try
        {
            writeStreamToFile(stream, outputPath, e -> {
                throw new CsvLineParsingFailedException("Failed to map object to csv line.", e);
            });
        }
        catch( CsvLineParsingFailedException e )
        {
            throw new CsvException("Exception occured while writing to CSV file.", e);
        }
    }

    /**
     * Writes a stream of objects to CSV, mapping each object to a new line,
     * handling each exception silently using a provided ExceptionHandler
     * and skipping the problematic lines.
     * Adds a header as a first line if withHeader option is set.
     *
     * @param stream          stream to write to CSV
     * @param outputPath      path of the output CSV file to write
     * @param exceptionHander handler of all exceptions which occur during writing or parsing
     */
    public void writeStreamToFile( Stream<T> stream, String outputPath, ExceptionHander exceptionHander )
    {
        try (OutputStream os = new FileOutputStream(outputPath);
                BufferedOutputStream bos = new BufferedOutputStream(os);
                PrintWriter printWriter = new PrintWriter(os))
        {
            if( header != null )
            {
                printWriter.println(header);
            }

            stream.map(object -> mapToCsvQuietly(object, exceptionHander))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(printWriter::write);
        }
        catch( IOException e )
        {
            exceptionHander.handleException(e);
        }
    }

    /**
     * Creates a Stream of POJOs from CSV file. Stops at first parsing error and rethrows it as
     * unchecked CsvLineParsingFailedException
     *
     * @param path - path to csv file
     * @return java Stream of objects
     * @throws CsvLineParsingFailedException when any kind of parsing error occurs
     * @throws CsvException                  when stream can not be opened or read for some reason(eg. file does not exist)
     */
    public Stream<T> readFileAsStream( String path ) throws CsvException
    {
        return readFileAsStream(path, e -> {
            throw new CsvLineParsingFailedException("Failed to map csv line to object.", e);
        });
    }

    /**
     * Creates a Stream of POJOs from CSV file. A custom exception handler can be provided
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
            Stream<String> lines = Files.lines(Paths.get(path));
            lines = skipHeaderIfNeeded(lines);
            lines = skipEmptyLinesIfNeeded(lines);
            return lines.map(line -> mapToObjectQuietly(line, exceptionHander))
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }
        catch( IOException e )
        {
            exceptionHander.handleException(e);
            return Stream.empty();
        }
    }

    private Stream<String> skipHeaderIfNeeded( Stream<String> lines )
    {
        return header != null ? lines.skip(1) : lines;
    }

    private Stream<String> skipEmptyLinesIfNeeded( Stream<String> lines )
    {
        return skipEmptyLines ? lines.filter(line -> !line.trim().isEmpty()) : lines;
    }

    public static class CsvBuilder<T>
    {

        private final Class<T> schemaClass;
        private char separator = ',';
        private boolean withHeader = false;
        private Locale locale = Locale.getDefault();
        private boolean skipEmptyLines = false;

        public CsvBuilder( Class<T> schemaClass )
        {
            this.schemaClass = schemaClass;
        }

        public CsvBuilder<T> withSeparator( char separator )
        {
            this.separator = separator;
            return this;
        }

        public CsvBuilder<T> withHeader()
        {
            this.withHeader = true;
            return this;
        }

        public CsvBuilder<T> withLocale( Locale locale )
        {
            this.locale = locale;
            return this;
        }

        public CsvBuilder<T> skipEmptyLines()
        {
            this.skipEmptyLines = true;
            return this;
        }

        public Csv<T> build()
        {
            CsvMapper mapper = new CsvMapper();
            mapper.setLocale(locale);
            CsvSchema schema = createSchema(mapper);
            ObjectReader reader = mapper.readerFor(schemaClass).with(schema).with(locale);
            ObjectWriter writer = mapper.writer(schema).with(locale);
            String header = withHeader ? getHeader(schema) : null;
            return new Csv<>(writer, reader, header, skipEmptyLines);
        }

        private CsvSchema createSchema( CsvMapper mapper )
        {
            return mapper.schemaFor(schemaClass).withColumnSeparator(separator);
        }

        private String getHeader( CsvSchema schema )
        {
            StringBuilder header = new StringBuilder();

            if( schema.size() > 0 )
            {
                header.append(schema.column(0).getName());
                for( int i = 1; i < schema.size(); i++ )
                {
                    header.append(schema.getColumnSeparator());
                    header.append(schema.column(i).getName());
                }
            }
            return header.toString();
        }


    }

    /**
     * Internal interface used to pass exceptions from streams
     */
    public interface ExceptionHander
    {
        void handleException( Exception e );
    }

}
