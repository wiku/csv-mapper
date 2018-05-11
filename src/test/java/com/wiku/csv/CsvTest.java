package com.wiku.csv;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class CsvTest
{

    private static final String NEWLINE = System.lineSeparator();
    private static final String EXCEPTION_MESSAGE = "Failed to write lines due to following errors: com.fasterxml.jackson.databind.JsonMappingException: (was java.lang.IllegalArgumentException) (through reference chain: com.wiku.csv.CsvTest$1[\"name\"]),";

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TestClass object1 = new TestClass("a", 1);
    private TestClass object2 = new TestClass("b", 2);
    private File expectedOutput = new File("src/test/resources/output.csv");

    private Csv<User> csv = Csv.from(User.class).build();

    @Test public void canWriteCsvLine() throws CsvException
    {
        Csv csv = Csv.from(TestClass.class).withSeparator(';').build();
        assertEquals("\"my text\";a;1.0" + NEWLINE, csv.mapToCsv(object1));
        assertEquals("\"my text\";b;2.0" + NEWLINE, csv.mapToCsv(object2));
    }

    @Test public void canReadCsvLine() throws CsvException
    {
        TestClass object1 = new TestClass("a", 1);

        Csv csv = Csv.from(TestClass.class).withSeparator(';').build();
        assertEquals(object1, csv.mapToObject("\"my text\";a;1"));
    }

    @Test public void readmeTest() throws CsvException, IOException
    {
        User user = new User("John", "Smith");
        String csvLine = csv.mapToCsv(user);
        User userFromCsv = csv.mapToObject("Steven,Hawking");
    }

    @Test public void canParseSilentlyAndCollectExceptionUsingHandler()
    {
        List<Exception> exceptionList = new ArrayList<>();

        Optional<User> optional = csv.mapToObjectQuietly("a,b,c" + NEWLINE, exceptionList::add);
        assertFalse(optional.isPresent());
        assertEquals(1, exceptionList.size());
    }

    @Test public void canGetObjectStreamFromCsv() throws IOException
    {
        List<String> names = csv.readFileAsStream("src/test/resources/users.csv")
                .map(User::getName)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("Steven", "Nassim", "Richard", "Stanislaw"), names);
    }

    @Test public void canGetObjectStreamFromCsvWithHeaders() throws IOException
    {
        Csv<User> csv = Csv.from(User.class).withHeader().build();
        List<String> names = csv.readFileAsStream("src/test/resources/users_headers.csv")
                .map(User::getName)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("Steven", "Nassim", "Richard", "Stanislaw"), names);
    }

    @Test public void canCollectStreamOfObjectsToCsv() throws CsvException, IOException
    {
        File outputFile = temporaryFolder.newFile();

        Stream<TestClass> testObjectStream = Arrays.asList(object1, object2).stream();

        Csv csv = Csv.from(TestClass.class).withSeparator(';').build();
        csv.writeStreamToFile(testObjectStream, outputFile.getPath());

        assertThat(outputFile).exists().hasSameContentAs(expectedOutput);
    }

    @Test public void canCollectExceptionsOccuringDuringWriteStreamToFile() throws CsvException, IOException
    {
        File outputFile = temporaryFolder.newFile();

        TestClass problematicObject = createElementCausingParsingIssue();

        Stream<TestClass> testObjectStream = Arrays.asList(object1, problematicObject, object2)
                .stream();

        Optional<Exception> exceptionThrown = writeToFileAndReturnExceptionThrown(testObjectStream, outputFile);

        assertThat(exceptionThrown).get().isInstanceOf(CsvException.class);
        assertThat(exceptionThrown.get()).hasMessageContaining(EXCEPTION_MESSAGE);
        assertThat(outputFile).exists().hasSameContentAs(expectedOutput);
    }

    @Test public void canGetHeaders()
    {
        assertThat(csv.getHeader()).isEqualTo("name,surname");
    }

    private Optional<Exception> writeToFileAndReturnExceptionThrown( Stream<TestClass> streamToWrite, File outputFile )
    {
        try
        {
            Csv csv = Csv.from(TestClass.class).withSeparator(';').withHeader().build();
            csv.writeStreamToFile(streamToWrite, outputFile.getPath());
            return Optional.empty();
        }
        catch( Exception e )
        {
            return Optional.of(e);
        }
    }

    private TestClass createElementCausingParsingIssue()
    {
        return new TestClass()
        {
            @Override public String getName()
            {
                throw new IllegalArgumentException();
            }
        };
    }

    public static class TestClass
    {

        private String name;
        private double number;

        @JsonUnwrapped InnerClass innerClass = new InnerClass();

        public void setNumber( double number )
        {
            this.number = number;
        }

        public InnerClass getInnerClass()
        {
            return innerClass;
        }

        public void setInnerClass( InnerClass innerClass )
        {
            this.innerClass = innerClass;
        }

        public TestClass()
        {
        }

        @Override public boolean equals( Object o )
        {
            if( this == o )
                return true;
            if( o == null || getClass() != o.getClass() )
                return false;
            TestClass testClass = (TestClass)o;
            return number == testClass.number && Objects.equals(name, testClass.name) && Objects.equals(innerClass,
                    testClass.innerClass);
        }

        @Override public int hashCode()
        {

            return Objects.hash(name, number, innerClass);
        }

        public TestClass( String name, double number )
        {
            this.name = name;
            this.number = number;
        }

        public String getName()
        {
            return name;
        }

        public void setName( String name )
        {
            this.name = name;
        }

        public double getNumber()
        {
            return number;
        }

        public static class InnerClass
        {
            public String myText = "my text";

            @Override public boolean equals( Object o )
            {
                if( this == o )
                    return true;
                if( o == null || getClass() != o.getClass() )
                    return false;
                InnerClass that = (InnerClass)o;
                return Objects.equals(myText, that.myText);
            }

            @Override public int hashCode()
            {

                return Objects.hash(myText);
            }
        }
    }

    private static class User
    {
        private String name;
        private String surname;

        @Override public String toString()
        {
            return "User{" + "name='" + name + '\'' + ", surname='" + surname + '\'' + '}';
        }

        public User()
        {
        }

        public User( String name, String surname )
        {
            this.name = name;
            this.surname = surname;
        }

        public String getSurname()
        {
            return surname;
        }

        public void setSurname( String surname )
        {
            this.surname = surname;
        }

        public String getName()
        {
            return name;
        }

        public void setName( String name )
        {
            this.name = name;
        }

    }
}
