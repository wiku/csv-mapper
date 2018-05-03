# csv-mapper
Object to CSV mapping library

The purpose of this project is to provide a simplified way of mapping POJOs to CSV and back using jackson-dataformat-csv.
This is useful for reading/writing CSV files line by line.


## Features
Reading and write CSV without any special magic:


1. Use simple POJO, eg.
```
User user = new User("John", "Smith");
```
2. Create Csv instance using builder pattern:
```
Csv<User> csv = Csv.from(User.class).build();
```
3. Map to CSV:
```
String csvLine = csv.mapToCsv(user);
```
4. Map from CSV:
```
User user = csv.mapToObject("Steven,Hawking\n");
```
5. Easy API to read CSV file as Java Stream):
```
csv.readFileAsStream("users.csv").map(User::getName).forEach(System.out::println);
```
6. "Silent" versions of above methods for simplified usage with Java or Reactive streams, which return Optional.empty() when line cannot be parsed:
```
Optional<User> user = csv.mapToObjectQuietly("A,S,1,2", Exception::printStackTrace);
```
