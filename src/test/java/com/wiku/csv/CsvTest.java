package com.wiku.csv;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class CsvTest
{

    @Test public void canWriteCsvLine() throws CsvException
    {
        TestClass object1 = new TestClass("a", 1);
        TestClass object2 = new TestClass("b", 2);

        Csv csv = Csv.from(TestClass.class).withSeparator(';').withNewline().build();
        assertEquals("\"my text\";a;1\n", csv.mapToCsv(object1));
        assertEquals("\"my text\";b;2\n", csv.mapToCsv(object2));
    }

    @Test public void canReadCsvLine() throws CsvException
    {
        TestClass object1 = new TestClass("a", 1);

        Csv csv = Csv.from(TestClass.class).withSeparator(';').build();
        assertEquals(object1, csv.mapToObject("\"my text\";a;1"));
    }



    public static class TestClass
    {

        private String name;
        private int number;

        @JsonUnwrapped InnerClass innerClass = new InnerClass();

        public void setNumber( int number )
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

        public TestClass( String name, int number )
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

        public int getNumber()
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
}
