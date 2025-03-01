import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {
    private String name;
    private String catalogFile;
    private Map<String, Table> tables;
    private Map<String, String> tableFiles; // Maps table names to their file paths
    
    /**
     * Creates a new empty database with the given name
     */
    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
        this.tableFiles = new HashMap<>();
        this.catalogFile = name + ".db";
    }
    
    /**
     * Loads a database from its catalog file
     */
    public static Database load(String catalogFile) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(catalogFile))) {
            String name = reader.readLine();
            Database db = new Database(name);
            db.catalogFile = catalogFile;
            
            int tableCount = Integer.parseInt(reader.readLine());
            for (int i = 0; i < tableCount; i++) {
                String[] parts = reader.readLine().split(":");
                String tableName = parts[0];
                String tableFile = parts[1];
                
                // Load the table from its file
                Table table = Table.loadFromFile(tableFile);
                
                // Add it to the database
                db.tables.put(tableName, table);
                db.tableFiles.put(tableName, tableFile);
            }
            
            return db;
        }
    }
    
    /**
     * Saves the database catalog file
     */
    public void save() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(catalogFile))) {
            writer.write(name);
            writer.newLine();
            
            writer.write(Integer.toString(tables.size()));
            writer.newLine();
            
            for (Map.Entry<String, String> entry : tableFiles.entrySet()) {
                writer.write(entry.getKey() + ":" + entry.getValue());
                writer.newLine();
            }
        }
    }
    
    /**
     * Saves the database to a new catalog file
     */
    public void saveAs(String newCatalogFile) throws IOException {
        String oldCatalogFile = this.catalogFile;
        this.catalogFile = newCatalogFile;
        try {
            save();
        } catch (IOException e) {
            this.catalogFile = oldCatalogFile; // Restore on failure
            throw e;
        }
    }
    
    /**
     * @return the database name
     */
    public String getName() {
        return name;
    }
    
    /**
     * @return list of table names in the database
     */
    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }
    
    /**
     * Gets a table by name
     * @throws IllegalArgumentException if the table doesn't exist
     */
    public Table getTable(String name) {
        Table table = tables.get(name);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + name);
        }
        return table;
    }
    
    /**
     * Checks if a table exists in the database
     */
    public boolean hasTable(String name) {
        return tables.containsKey(name);
    }
    
    /**
     * Creates a new empty table with the given name and columns
     * @throws IllegalArgumentException if a table with the same name already exists
     */
    public void createTable(String name, List<String> columnNames, List<DataType> columnTypes) {
        if (tables.containsKey(name)) {
            throw new IllegalArgumentException("Table already exists: " + name);
        }
        
        Table table = new Table(name, columnNames, columnTypes);
        
        // Generate a file path for the table
        String tableFile = name + "_" + System.currentTimeMillis() + ".tbl";
        
        tables.put(name, table);
        tableFiles.put(name, tableFile);
    }
    
    /**
     * Imports a table from a file
     * @throws IOException if there's an error reading the file
     * @throws IllegalArgumentException if a table with the same name already exists
     */
    public void importTable(String fileName) throws IOException {
        Table table = Table.loadFromFile(fileName);
        String tableName = table.getName();
        
        if (tables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table already exists: " + tableName);
        }
        
        // Generate a unique file path for the table
        String tableFile = tableName + "_" + System.currentTimeMillis() + ".tbl";
        
        // Save the table to our storage location
        table.saveToFile(tableFile);
        
        tables.put(tableName, table);
        tableFiles.put(tableName, tableFile);
    }
    
    /**
     * Exports a table to a file
     * @throws IOException if there's an error writing to the file
     */
    public void exportTable(String tableName, String fileName) throws IOException {
        Table table = getTable(tableName);
        table.saveToFile(fileName);
    }
    
    /**
     * Renames a table
     * @throws IllegalArgumentException if the source table doesn't exist or the target name is taken
     */
    public void renameTable(String oldName, String newName) {
        if (!tables.containsKey(oldName)) {
            throw new IllegalArgumentException("Table not found: " + oldName);
        }
        
        if (tables.containsKey(newName)) {
            throw new IllegalArgumentException("A table named '" + newName + "' already exists");
        }
        
        Table table = tables.remove(oldName);
        table.setName(newName);
        tables.put(newName, table);
        
        String tableFile = tableFiles.remove(oldName);
        tableFiles.put(newName, tableFile);
    }
    
    /**
     * Deletes a table from the database
     * @throws IllegalArgumentException if the table doesn't exist
     */
    public void dropTable(String name) {
        if (!tables.containsKey(name)) {
            throw new IllegalArgumentException("Table not found: " + name);
        }
        
        tables.remove(name);
        String tableFile = tableFiles.remove(name);
        
        // Optionally delete the file
        new File(tableFile).delete();
    }
    
    /**
     * Performs an inner join of two tables
     * Creates a new table with a unique name
     * @return the name of the new table
     */
    public String innerJoin(String table1Name, int column1, String table2Name, int column2) {
        Table table1 = getTable(table1Name);
        Table table2 = getTable(table2Name);
        
        // Validate column indices
        if (column1 < 0 || column1 >= table1.getColumnCount()) {
            throw new IllegalArgumentException("Invalid column index for table " + table1Name + ": " + column1);
        }
        
        if (column2 < 0 || column2 >= table2.getColumnCount()) {
            throw new IllegalArgumentException("Invalid column index for table " + table2Name + ": " + column2);
        }
        
        // Create column names and types for the joined table
        List<String> joinedColumnNames = new ArrayList<>();
        List<DataType> joinedColumnTypes = new ArrayList<>();
        
        // Add columns from table 1
        for (int i = 0; i < table1.getColumnCount(); i++) {
            joinedColumnNames.add(table1Name + "_" + table1.getColumnName(i));
            joinedColumnTypes.add(table1.getColumnType(i));
        }
        
        // Add columns from table 2
        for (int i = 0; i < table2.getColumnCount(); i++) {
            joinedColumnNames.add(table2Name + "_" + table2.getColumnName(i));
            joinedColumnTypes.add(table2.getColumnType(i));
        }
        
        // Generate a unique name for the join result
        String joinedTableName = "join_" + table1Name + "_" + table2Name + "_" + System.currentTimeMillis();
        
        // Create the new table
        Table joinedTable = new Table(joinedTableName, joinedColumnNames, joinedColumnTypes);
        
        // Get all rows from both tables
        List<List<Cell>> rows1 = table1.getAllRows();
        List<List<Cell>> rows2 = table2.getAllRows();
        
        // Perform the inner join
        for (List<Cell> row1 : rows1) {
            Cell cell1 = row1.get(column1);
            
            for (List<Cell> row2 : rows2) {
                Cell cell2 = row2.get(column2);
                
                // If both cells are NULL or their values match
                if ((cell1.isNull() && cell2.isNull()) || 
                    (!cell1.isNull() && !cell2.isNull() && cell1.getValue().equals(cell2.getValue()))) {
                    
                    // Create a joined row
                    List<String> joinedRowValues = new ArrayList<>();
                    
                    // Add values from row1
                    for (Cell cell : row1) {
                        joinedRowValues.add(cell.isNull() ? null : cell.getValue());
                    }
                    
                    // Add values from row2
                    for (Cell cell : row2) {
                        joinedRowValues.add(cell.isNull() ? null : cell.getValue());
                    }
                    
                    // Insert the joined row
                    joinedTable.insertRow(joinedRowValues);
                }
            }
        }
        
        // Generate a file path for the joined table
        String joinedTableFile = joinedTableName + ".tbl";
        
        // Add the joined table to the database
        tables.put(joinedTableName, joinedTable);
        tableFiles.put(joinedTableName, joinedTableFile);
        
        try {
            // Save the joined table
            joinedTable.saveToFile(joinedTableFile);
        } catch (IOException e) {
            System.err.println("Warning: Failed to save joined table to file: " + e.getMessage());
        }
        
        return joinedTableName;
    }
    

    public void saveAllTables() throws IOException {
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            Table table = entry.getValue();
            String tableFile = tableFiles.get(tableName);
            
            table.saveToFile(tableFile);
        }
        
        // Save the catalog file
        save();
    }
    
    public void close() throws IOException {
        saveAllTables();
    }
}