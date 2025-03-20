import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DatabaseCLI {
    private Database currentDatabase;
    private boolean running;
    private final Scanner scanner;
    private static final int PAGE_SIZE = 20;
    
    public DatabaseCLI() {
        this.scanner = new Scanner(System.in);
        this.running = true;
    }
    
    public void start() {
        System.out.println("Simple Database System - Type 'help' for available commands");
        
        while (running) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) {
                continue;
            }
            
            try {
                processCommand(input);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }
        
        scanner.close();
    }
    
    private void processCommand(String input) throws IOException {
        String[] parts = parseCommandLine(input);
        String command = parts[0].toLowerCase();
        String[] args = Arrays.copyOfRange(parts, 1, parts.length);
        
        switch (command) {
            case "help" -> showHelp();
                
            case "open" -> {
                if (args.length != 1) {
                    System.err.println("Usage: open <database_file>");
                    return;
                }
                openDatabase(args[0]);
            }
                
            case "create" -> {
                if (args.length != 1) {
                    System.err.println("Usage: create <database_name>");
                    return;
                }
                createDatabase(args[0]);
            }
                
            case "close" -> closeDatabase();
                
            case "save" -> saveDatabase();
                
            case "saveas" -> {
                if (args.length != 1) {
                    System.err.println("Usage: saveas <database_file>");
                    return;
                }
                saveDatabaseAs(args[0]);
            }
                
            case "import" -> {
                if (args.length != 1) {
                    System.err.println("Usage: import <file_name>");
                    return;
                }
                importTable(args[0]);
            }
                
            case "showtables" -> showTables();
                
            case "describe" -> {
                if (args.length != 1) {
                    System.err.println("Usage: describe <table_name>");
                    return;
                }
                describeTable(args[0]);
            }
                
            case "print" -> {
                if (args.length != 1) {
                    System.err.println("Usage: print <table_name>");
                    return;
                }
                printTable(args[0]);
            }
                
            case "export" -> {
                if (args.length != 2) {
                    System.err.println("Usage: export <table_name> <file_name>");
                    return;
                }
                exportTable(args[0], args[1]);
            }
                
            case "select" -> {
                if (args.length != 3) {
                    System.err.println("Usage: select <column_n> <value> <table_name>");
                    return;
                }
                selectFromTable(args[2], Integer.parseInt(args[0]), args[1]);
            }
                
            case "addcolumn" -> {
                if (args.length != 3) {
                    System.err.println("Usage: addcolumn <table_name> <column_name> <column_type>");
                    return;
                }
                addColumn(args[0], args[1], args[2]);
            }
                
            case "update" -> {
                if (args.length != 5) {
                    System.err.println("Usage: update <table_name> <search_column_n> <search_value> <target_column_n> <target_value>");
                    return;
                }
                updateTable(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]), args[4]);
            }
                
            case "delete" -> {
                if (args.length != 3) {
                    System.err.println("Usage: delete <table_name> <search_column_n> <search_value>");
                    return;
                }
                deleteFromTable(args[0], Integer.parseInt(args[1]), args[2]);
            }
                
            case "insert" -> {
                if (args.length < 1) {
                    System.err.println("Usage: insert <table_name> <value1> <value2> ...");
                    return;
                }
                insertIntoTable(args[0], Arrays.copyOfRange(args, 1, args.length));
            }
                
            case "innerjoin" -> {
                if (args.length != 4) {
                    System.err.println("Usage: innerjoin <table1> <column_n1> <table2> <column_n2>");
                    return;
                }
                innerJoin(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]));
            }
                
            case "rename" -> {
                if (args.length != 2) {
                    System.err.println("Usage: rename <old_name> <new_name>");
                    return;
                }
                renameTable(args[0], args[1]);
            }
                
            case "count" -> {
                if (args.length != 3) {
                    System.err.println("Usage: count <table_name> <search_column_n> <search_value>");
                    return;
                }
                countRows(args[0], Integer.parseInt(args[1]), args[2]);
            }
                
            case "aggregate" -> {
                if (args.length != 5) {
                    System.err.println("Usage: aggregate <table_name> <search_column_n> <search_value> <target_column_n> <operation>");
                    return;
                }
                aggregate(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]), args[4]);
            }
                
            case "exit" -> exit();
                
            default -> {
                System.err.println("Unknown command: " + command);
                System.err.println("Type 'help' for available commands");
            }
        }
    }
    
    private String[] parseCommandLine(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean inQuotes = false;
        boolean escapeNext = false;
        
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            
            if (escapeNext) {
                currentToken.append(c);
                escapeNext = false;
            } else if (c == '\\' && inQuotes) {
                escapeNext = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
                currentToken.append(c);
            } else if (Character.isWhitespace(c) && !inQuotes) {
                if (currentToken.length() > 0) {
                    tokens.add(currentToken.toString());
                    currentToken = new StringBuilder();
                }
            } else {
                currentToken.append(c);
            }
        }
        
        if (currentToken.length() > 0) {
            tokens.add(currentToken.toString());
        }
        
        return tokens.toArray(String[]::new);
    }
    
    private void showHelp() {
        System.out.println("Available commands:");
        System.out.println("  help                                      - Show this help message");
        System.out.println("  open <database_file>                      - Open an existing database");
        System.out.println("  create <database_name>                    - Create a new database");
        System.out.println("  close                                     - Close the current database");
        System.out.println("  save                                      - Save the current database");
        System.out.println("  saveas <database_file>                    - Save the database to a new file");
        System.out.println("  exit                                      - Exit the program");
        System.out.println();
        System.out.println("Table Commands (require an open database):");
        System.out.println("  import <file_name>                        - Import a table from a file");
        System.out.println("  showtables                                - Show all tables in the database");
        System.out.println("  describe <table_name>                     - Show column information for a table");
        System.out.println("  print <table_name>                        - Display all rows from a table");
        System.out.println("  export <table_name> <file_name>           - Export a table to a file");
        System.out.println("  select <column_n> <value> <table_name>    - Show rows where column matches value");
        System.out.println("  addcolumn <table_name> <column_name> <column_type> - Add a new column");
        System.out.println("  update <table_name> <search_column_n> <search_value> <target_column_n> <target_value>");
        System.out.println("                                            - Update rows in a table");
        System.out.println("  delete <table_name> <search_column_n> <search_value>");
        System.out.println("                                            - Delete rows from a table");
        System.out.println("  insert <table_name> <value1> <value2> ... - Insert a new row");
        System.out.println("  innerjoin <table1> <column_n1> <table2> <column_n2>");
        System.out.println("                                            - Join two tables");
        System.out.println("  rename <old_name> <new_name>              - Rename a table");
        System.out.println("  count <table_name> <search_column_n> <search_value>");
        System.out.println("                                            - Count matching rows");
        System.out.println("  aggregate <table_name> <search_column_n> <search_value> <target_column_n> <operation>");
        System.out.println("                                            - Perform aggregation (sum, product, maximum, minimum)");
    }
    
    private void openDatabase(String filename) throws IOException {
        if (currentDatabase != null) {
            closeDatabase();
        }
        
        currentDatabase = Database.load(filename);
        System.out.println("Database '" + currentDatabase.getName() + "' opened successfully");
    }
    
    private void createDatabase(String name) {
        if (currentDatabase != null) {
            try {
                closeDatabase();
            } catch (IOException e) {
                System.err.println("Warning: Failed to close current database: " + e.getMessage());
            }
        }
        
        currentDatabase = new Database(name);
        System.out.println("Database '" + name + "' created successfully");
    }
    
    private void closeDatabase() throws IOException {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        String name = currentDatabase.getName();
        currentDatabase.close();
        currentDatabase = null;
        System.out.println("Database '" + name + "' closed successfully");
    }
    
    private void saveDatabase() throws IOException {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        currentDatabase.save();
        System.out.println("Database saved successfully");
    }
    
    private void saveDatabaseAs(String filename) throws IOException {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        currentDatabase.saveAs(filename);
        System.out.println("Database saved as '" + filename + "' successfully");
    }
    
    private void importTable(String filename) throws IOException {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        currentDatabase.importTable(filename);
        System.out.println("Table imported successfully");
    }
    
    private void showTables() {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        List<String> tableNames = currentDatabase.getTableNames();
        if (tableNames.isEmpty()) {
            System.out.println("No tables in the database");
            return;
        }
        
        System.out.println("Tables in database '" + currentDatabase.getName() + "':");
        for (String name : tableNames) {
            System.out.println("  " + name);
        }
    }
    
    private void describeTable(String tableName) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            List<String> columnNames = table.getColumnNames();
            List<DataType> columnTypes = table.getColumnTypes();
            
            System.out.println("Table: " + tableName);
            System.out.println("Columns:");
            System.out.println("Index | Name | Type");
            System.out.println("----- | ---- | ----");
            
            for (int i = 0; i < columnNames.size(); i++) {
                System.out.printf("%-5d | %-4s | %s%n", i, columnNames.get(i), columnTypes.get(i));
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void printTable(String tableName) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            List<List<Cell>> rows = table.getAllRows();
            displayRowsWithPagination(table, rows);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void exportTable(String tableName, String filename) throws IOException {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        currentDatabase.exportTable(tableName, filename);
        System.out.println("Table '" + tableName + "' exported to '" + filename + "' successfully");
    }
    
    private void selectFromTable(String tableName, int columnIndex, String value) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            List<List<Cell>> selectedRows = table.selectRows(columnIndex, value.equals("NULL") ? null : value);
            
            if (selectedRows.isEmpty()) {
                System.out.println("No matching rows found");
                return;
            }
            
            System.out.println("Found " + selectedRows.size() + " matching rows:");
            displayRowsWithPagination(table, selectedRows);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private DataType parseDataType(String type) {
        switch (type.toLowerCase()) {
            case "integer", "int" -> {
                return new IntegerType();
            }
            case "float", "double" -> {
                return new FloatType();
            }
            case "string", "str" -> {
                return new StringType();
            }
            default -> throw new IllegalArgumentException("Unknown data type: " + type);
        }
    }
    
    private void addColumn(String tableName, String columnName, String typeName) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            DataType type = parseDataType(typeName);
            table.addColumn(columnName, type);
            System.out.println("Column '" + columnName + "' added to table '" + tableName + "'");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void updateTable(String tableName, int searchColumnIndex, String searchValue, int targetColumnIndex, String targetValue) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            table.updateRows(searchColumnIndex, searchValue.equals("NULL") ? null : searchValue, 
                            targetColumnIndex, targetValue.equals("NULL") ? null : targetValue);
            System.out.println("Table updated successfully");
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void deleteFromTable(String tableName, int searchColumnIndex, String searchValue) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            int rowCount = table.getRowCount();
            table.deleteRows(searchColumnIndex, searchValue.equals("NULL") ? null : searchValue);
            int deletedCount = rowCount - table.getRowCount();
            System.out.println(deletedCount + " rows deleted from table '" + tableName + "'");
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void insertIntoTable(String tableName, String[] values) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            List<String> valueList = new ArrayList<>();
            
            for (String value : values) {
                valueList.add(value.equals("NULL") ? null : value);
            }
            
            table.insertRow(valueList);
            System.out.println("New row inserted into table '" + tableName + "'");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void innerJoin(String table1Name, int column1, String table2Name, int column2) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            String joinedTableName = currentDatabase.innerJoin(table1Name, column1, table2Name, column2);
            System.out.println("Tables joined successfully, new table: '" + joinedTableName + "'");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void renameTable(String oldName, String newName) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            currentDatabase.renameTable(oldName, newName);
            System.out.println("Table renamed from '" + oldName + "' to '" + newName + "'");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void countRows(String tableName, int columnIndex, String value) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            int count = table.countRows(columnIndex, value.equals("NULL") ? null : value);
            System.out.println("Count: " + count);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void aggregate(String tableName, int searchColumnIndex, String searchValue, int targetColumnIndex, String operation) {
        if (currentDatabase == null) {
            System.err.println("No database is currently open");
            return;
        }
        
        try {
            Table table = currentDatabase.getTable(tableName);
            Object result = table.aggregate(searchColumnIndex, 
                                          searchValue.equals("NULL") ? null : searchValue, 
                                          targetColumnIndex, operation);
            
            if (result == null) {
                System.out.println("No result (empty set or all NULL values)");
            } else {
                System.out.println(operation + ": " + result);
            }
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            System.err.println(e.getMessage());
        }
    }
    
    private void displayRowsWithPagination(Table table, List<List<Cell>> rows) {
        if (rows.isEmpty()) {
            System.out.println("No rows to display");
            return;
        }
        
        List<String> columnNames = table.getColumnNames();
        
        int[] columnWidths = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            columnWidths[i] = columnNames.get(i).length();
        }
        
        for (List<Cell> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                Cell cell = row.get(i);
                String value = cell.toString();
                columnWidths[i] = Math.max(columnWidths[i], value.length());
            }
        }
        
        int currentPage = 0;
        int totalPages = (rows.size() + PAGE_SIZE - 1) / PAGE_SIZE;
        boolean viewing = true;
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        while (viewing) {
            for (int i = 0; i < columnNames.size(); i++) {
                System.out.print(String.format("%-" + (columnWidths[i] + 2) + "s", columnNames.get(i)));
            }
            System.out.println();
            
            for (int i = 0; i < columnNames.size(); i++) {
                for (int j = 0; j < columnWidths[i] + 2; j++) {
                    System.out.print("-");
                }
            }
            System.out.println();
            
            int startRow = currentPage * PAGE_SIZE;
            int endRow = Math.min(startRow + PAGE_SIZE, rows.size());
            
            for (int rowIndex = startRow; rowIndex < endRow; rowIndex++) {
                List<Cell> row = rows.get(rowIndex);
                for (int i = 0; i < row.size(); i++) {
                    System.out.print(String.format("%-" + (columnWidths[i] + 2) + "s", row.get(i).toString()));
                }
                System.out.println();
            }
            
            System.out.println();
            System.out.println("Page " + (currentPage + 1) + " of " + totalPages);
            System.out.println("Commands: (n)ext page, (p)revious page, (q)uit viewing");
            System.out.print("> ");
            
            try {
                String command = reader.readLine().trim().toLowerCase();
                switch (command) {
                    case "n", "next" -> {
                        if (currentPage < totalPages - 1) {
                            currentPage++;
                        } else {
                            System.out.println("Already at the last page");
                        }
                    }
                    case "p", "previous" -> {
                        if (currentPage > 0) {
                            currentPage--;
                        } else {
                            System.out.println("Already at the first page");
                        }
                    }
                    case "q", "quit" -> viewing = false;
                    default -> System.out.println("Unknown command: " + command);
                }
            } catch (IOException e) {
                System.err.println("Error reading input: " + e.getMessage());
                viewing = false;
            }
            
            if (viewing) {
                System.out.println("\n\n");
            }
        }
    }
    
    private void exit() throws IOException {
        if (currentDatabase != null) {
            closeDatabase();
        }
        
        running = false;
        System.out.println("Exiting...");
    }
    
    public static void main(String[] args) {
        DatabaseCLI cli = new DatabaseCLI();
        cli.start();
    }
}
