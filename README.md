# csv-mapper
Object to CSV mapping library

The purpose of this project is to provide a simplified way of mapping POJOs to CSV and back using jackson-dataformat-csv.
This is useful for reading/writing CSV files line by line.


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