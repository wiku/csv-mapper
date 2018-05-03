package com.wiku.csv;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CsvTest
{

    private static final String NEWLINE = System.lineSeparator();

    @Test public void canWriteCsvLine() throws CsvException
    {
        TestClass object1 = new TestClass("a", 1);
        TestClass object2 = new TestClass("b", 2);

        Csv csv = Csv.from(TestClass.class).withSeparator(';').withNewline().build();
        assertEquals("\"my text\";a;1.0" + NEWLINE, csv.mapToCsv(object1));
        assertEquals("\"my text\";b;2.0"+ NEWLINE, csv.mapToCsv(object2));
    }

    @Test public void canReadCsvLine() throws CsvException
    {
        TestClass object1 = new TestClass("a", 1);

        Csv csv = Csv.from(TestClass.class).withSeparator(';').build();
        assertEquals(object1, csv.mapToObject("\"my text\";a;1"));
    }

    @Test public void readmeTest() throws CsvException, IOException
    {
        Csv<User> csv = Csv.from(User.class).build();
        User user = new User("John", "Smith");
        String csvLine = csv.mapToCsv(user);
        User userFromCsv = csv.mapToObject("Steven,Hawking");
        Optional<User> optionalUser = csv.mapToObjectQuietly("Steven,Hawking", Exception::printStackTrace);
        System.out.println(userFromCsv.getName());
        Optional<User> s = csv.mapToObjectQuietly("A,S,1,2", Exception::printStackTrace);
    }

    @Test public void canParseSilentlyAndCollectExceptionUsingHandler()
    {
        Csv<User> csv = Csv.from(User.class).build();
        List<Exception> exceptionList = new ArrayList<>();

        Optional<User> optional = csv.mapToObjectQuietly("a,b,c" + NEWLINE, exceptionList::add);
        assertFalse(optional.isPresent());
        assertEquals(1, exceptionList.size());
    }

    @Test
    public void canGetObjectStreamFromCsv() throws IOException
    {
        Csv<User> csv = Csv.from(User.class).build();
        List<String> names = csv.readFileAsStream("src/test/resources/users.csv")
                .map(User::getName)
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("Steven", "Nassim", "Richard", "Stanislaw"), names);
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
