# csv-mapper
Object to CSV mapping library

Simple adapter library for jackson-dataformat-csv is, which provides a simplified way of mapping CSV lines to/from POJOs and additional utility methods for reading whole CSV files as streams.


## Usage
1. Create Csv instance using builder pattern:
```
Csv<User> csv = Csv.from(User.class).build();
```
3. Map a simple POJO to CSV:
```
User user = new User("John", "Smith");
String csvLine = csv.mapToCsv(user);
```
4. Map from CSV to POJO:
```
User user = csv.mapToObject("Steven,Hawking\n");
```
5. Read CSV file as Java Stream of POJOs:
```
csv.readFileAsStream("users.csv").map(User::getName).forEach(System.out::println);
```