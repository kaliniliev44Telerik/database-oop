import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Table {
    private String name;
    private List<String> columnNames;
    private List<DataType> columnTypes;
    private List<List<Cell>> rows;
    
    /**
     * Create a new empty table with the given name
     */
    public Table(String name) {
        this.name = name;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        this.rows = new ArrayList<>();
    }

    /**
     * Create a table with name and predefined columns
     */
    public Table(String name, List<String> columnNames, List<DataType> columnTypes) {
        this.name = name;
        this.columnNames = new ArrayList<>(columnNames);
        this.columnTypes = new ArrayList<>(columnTypes);
        this.rows = new ArrayList<>();
        
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Column names and types must have the same length");
        }
    }
    
    public String getName() {
        return name;
    }
    

    public void setName(String name) {
        this.name = name;
    }
    
    public int getColumnCount() {
        return columnNames.size();
    }
    
    public int getRowCount() {
        return rows.size();
    }
    

    public String getColumnName(int index) {
        if (index < 0 || index >= columnNames.size()) {
            throw new IndexOutOfBoundsException("Column index out of range: " + index);
        }
        return columnNames.get(index);
    }
    
    public DataType getColumnType(int index) {
        if (index < 0 || index >= columnTypes.size()) {
            throw new IndexOutOfBoundsException("Column index out of range: " + index);
        }
        return columnTypes.get(index);
    }
    
    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }
    

    public List<DataType> getColumnTypes() {
        return new ArrayList<>(columnTypes);
    }
    

    public void addColumn(String name, DataType type) {
        columnNames.add(name);
        columnTypes.add(type);
        
        // Add NULL cells for this column to all existing rows
        for (List<Cell> row : rows) {
            row.add(new Cell(type));
        }
    }
    

    public void insertRow(List<String> values) {
        if (values.size() > columnTypes.size()) {
            throw new IllegalArgumentException("Too many values provided: " + values.size() + 
                                               " (expected at most " + columnTypes.size() + ")");
        }
        
        List<Cell> newRow = new ArrayList<>();
        
        for (int i = 0; i < values.size(); i++) {
            String value = values.get(i);
            DataType type = columnTypes.get(i);
            
            if (value == null || value.equals("NULL")) {
                newRow.add(new Cell(type));
            } else {
                newRow.add(new Cell(type, value));
            }
        }
        
        // Add NULL values for any remaining columns
        for (int i = values.size(); i < columnTypes.size(); i++) {
            newRow.add(new Cell(columnTypes.get(i)));
        }
        
        rows.add(newRow);
    }
    
    /**
     * Updates rows where the search column equals the search value
     * Sets the target column to the target value
     */
    public void updateRows(int searchColIndex, String searchValue, int targetColIndex, String targetValue) {
        validateColumnIndex(searchColIndex);
        validateColumnIndex(targetColIndex);
        
        DataType searchType = columnTypes.get(searchColIndex);
        DataType targetType = columnTypes.get(targetColIndex);
        
        if (searchValue != null && !searchValue.equals("NULL") && !searchType.validate(searchValue)) {
            throw new IllegalArgumentException("Search value '" + searchValue + 
                                               "' is not valid for column type " + searchType);
        }
        
        if (targetValue != null && !targetValue.equals("NULL") && !targetType.validate(targetValue)) {
            throw new IllegalArgumentException("Target value '" + targetValue + 
                                               "' is not valid for column type " + targetType);
        }
        
        boolean isSearchNull = (searchValue == null || searchValue.equals("NULL"));
        
        for (List<Cell> row : rows) {
            Cell searchCell = row.get(searchColIndex);
            
            boolean match = false;
            if (isSearchNull) {
                match = searchCell.isNull();
            } else {
                match = !searchCell.isNull() && searchCell.getValue().equals(searchValue);
            }
            
            if (match) {
                if (targetValue == null || targetValue.equals("NULL")) {
                    row.get(targetColIndex).setValue(null);
                } else {
                    row.get(targetColIndex).setValue(targetValue);
                }
            }
        }
    }
    

    public void deleteRows(int columnIndex, String value) {
        validateColumnIndex(columnIndex);
        
        DataType type = columnTypes.get(columnIndex);
        
        if (value != null && !value.equals("NULL") && !type.validate(value)) {
            throw new IllegalArgumentException("Value '" + value + 
                                               "' is not valid for column type " + type);
        }
        
        boolean isNull = (value == null || value.equals("NULL"));
        
        rows.removeIf(row -> {
            Cell cell = row.get(columnIndex);
            if (isNull) {
                return cell.isNull();
            } else {
                return !cell.isNull() && cell.getValue().equals(value);
            }
        });
    }
    
  
    public List<List<Cell>> selectRows(int columnIndex, String value) {
        validateColumnIndex(columnIndex);
        
        DataType type = columnTypes.get(columnIndex);
        
        if (value != null && !value.equals("NULL") && !type.validate(value)) {
            throw new IllegalArgumentException("Value '" + value + 
                                               "' is not valid for column type " + type);
        }
        
        boolean isNull = (value == null || value.equals("NULL"));
        List<List<Cell>> result = new ArrayList<>();
        
        for (List<Cell> row : rows) {
            Cell cell = row.get(columnIndex);
            boolean match = false;
            
            if (isNull) {
                match = cell.isNull();
            } else {
                match = !cell.isNull() && cell.getValue().equals(value);
            }
            
            if (match) {
                result.add(new ArrayList<>(row));
            }
        }
        
        return result;
    }
    

    public int countRows(int columnIndex, String value) {
        return selectRows(columnIndex, value).size();
    }
    
  
    public Object aggregate(int searchColIndex, String searchValue, int targetColIndex, String operation) {
        validateColumnIndex(searchColIndex);
        validateColumnIndex(targetColIndex);
        
        DataType targetType = columnTypes.get(targetColIndex);
        
        // Check if target column is numeric
        if (!(targetType instanceof IntegerType || targetType instanceof FloatType)) {
            throw new IllegalArgumentException("Target column must be numeric for aggregate operations");
        }
        
        List<List<Cell>> selectedRows = selectRows(searchColIndex, searchValue);
        
        if (selectedRows.isEmpty()) {
            return null;
        }
        
        // Filter out null values from target column
        List<Number> numericValues = new ArrayList<>();
        for (List<Cell> row : selectedRows) {
            Cell cell = row.get(targetColIndex);
            if (!cell.isNull()) {
                Object value = cell.getParsedValue();
                if (value instanceof Number) {
                    numericValues.add((Number) value);
                }
            }
        }
        
        if (numericValues.isEmpty()) {
            return null;
        }
        
        switch (operation.toLowerCase()) {
            case "sum":
                if (targetType instanceof IntegerType) {
                    int sum = 0;
                    for (Number num : numericValues) {
                        sum += num.intValue();
                    }
                    return sum;
                } else {
                    float sum = 0;
                    for (Number num : numericValues) {
                        sum += num.floatValue();
                    }
                    return sum;
                }
                
            case "product":
                if (targetType instanceof IntegerType) {
                    int product = 1;
                    for (Number num : numericValues) {
                        product *= num.intValue();
                    }
                    return product;
                } else {
                    float product = 1;
                    for (Number num : numericValues) {
                        product *= num.floatValue();
                    }
                    return product;
                }
                
            case "maximum":
                if (targetType instanceof IntegerType) {
                    int max = Integer.MIN_VALUE;
                    for (Number num : numericValues) {
                        max = Math.max(max, num.intValue());
                    }
                    return max;
                } else {
                    float max = Float.MIN_VALUE;
                    for (Number num : numericValues) {
                        max = Math.max(max, num.floatValue());
                    }
                    return max;
                }
                
            case "minimum":
                if (targetType instanceof IntegerType) {
                    int min = Integer.MAX_VALUE;
                    for (Number num : numericValues) {
                        min = Math.min(min, num.intValue());
                    }
                    return min;
                } else {
                    float min = Float.MAX_VALUE;
                    for (Number num : numericValues) {
                        min = Math.min(min, num.floatValue());
                    }
                    return min;
                }
                
            default:
                throw new IllegalArgumentException("Unknown aggregate operation: " + operation);
        }
    }
    

    public List<List<Cell>> getAllRows() {
        List<List<Cell>> result = new ArrayList<>();
        for (List<Cell> row : rows) {
            result.add(new ArrayList<>(row));
        }
        return result;
    }

    public void saveToFile(String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            // Write table name
            writer.write(name);
            writer.newLine();
            
            // Write column count
            writer.write(Integer.toString(columnNames.size()));
            writer.newLine();
            
            // Write column definitions
            for (int i = 0; i < columnNames.size(); i++) {
                writer.write(columnNames.get(i));
                writer.write(',');
                writer.write(columnTypes.get(i).toString());
                writer.newLine();
            }
            
            // Write row count
            writer.write(Integer.toString(rows.size()));
            writer.newLine();
            
            // Write rows
            for (List<Cell> row : rows) {
                StringBuilder rowStr = new StringBuilder();
                for (int i = 0; i < row.size(); i++) {
                    Cell cell = row.get(i);
                    if (i > 0) {
                        rowStr.append(',');
                    }
                    
                    if (cell.isNull()) {
                        rowStr.append("NULL");
                    } else {
                        if (columnTypes.get(i) instanceof StringType) {
                            rowStr.append(StringType.escapeString(cell.getValue()));
                        } else {
                            rowStr.append(cell.getValue());
                        }
                    }
                }
                writer.write(rowStr.toString());
                writer.newLine();
            }
        }
    }
    

    public static Table loadFromFile(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            // Read table name
            String name = reader.readLine();
            
            // Read column count
            int columnCount = Integer.parseInt(reader.readLine());
            
            // Read column definitions
            List<String> columnNames = new ArrayList<>();
            List<DataType> columnTypes = new ArrayList<>();
            
            for (int i = 0; i < columnCount; i++) {
                String[] parts = reader.readLine().split(",", 2);
                String columnName = parts[0];
                String typeName = parts[1];
                
                DataType type;
                switch (typeName) {
                    case "IntegerType":
                        type = new IntegerType();
                        break;
                    case "FloatType":
                        type = new FloatType();
                        break;
                    case "StringType":
                        type = new StringType();
                        break;
                    default:
                        throw new IOException("Unknown data type: " + typeName);
                }
                
                columnNames.add(columnName);
                columnTypes.add(type);
            }
            
            Table table = new Table(name, columnNames, columnTypes);
            
            // Read row count
            int rowCount = Integer.parseInt(reader.readLine());
            
            // Read rows
            for (int i = 0; i < rowCount; i++) {
                String line = reader.readLine();
                List<String> values = parseCSVLine(line, columnTypes);
                table.insertRow(values);
            }
            
            return table;
        }
    }
    
    /**
     * Helper method to parse a CSV line with proper handling of string values with commas
     */
    private static List<String> parseCSVLine(String line, List<DataType> columnTypes) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;
        boolean escapeNext = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (escapeNext) {
                currentValue.append(c);
                escapeNext = false;
            } else if (c == '\\' && inQuotes) {
                escapeNext = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
                currentValue.append(c);
            } else if (c == ',' && !inQuotes) {
                // End of value
                String value = currentValue.toString();
                if (value.equals("NULL")) {
                    values.add(null);
                } else if (columnTypes.get(values.size()) instanceof StringType && 
                          value.startsWith("\"") && value.endsWith("\"")) {
                    values.add(StringType.unescapeString(value));
                } else {
                    values.add(value);
                }
                currentValue = new StringBuilder();
            } else {
                currentValue.append(c);
            }
        }
        
        // Add the last value
        String value = currentValue.toString();
        if (value.equals("NULL")) {
            values.add(null);
        } else if (!values.isEmpty() && columnTypes.get(values.size()) instanceof StringType && 
                  value.startsWith("\"") && value.endsWith("\"")) {
            values.add(StringType.unescapeString(value));
        } else {
            values.add(value);
        }
        
        return values;
    }
    

    private void validateColumnIndex(int index) {
        if (index < 0 || index >= columnTypes.size()) {
            throw new IndexOutOfBoundsException("Column index out of range: " + index);
        }
    }
    

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(name).append("\n");
        
        // Calculate column widths
        int[] columnWidths = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            columnWidths[i] = columnNames.get(i).length();
        }
        
        // Get maximum width for each column based on data
        for (List<Cell> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                Cell cell = row.get(i);
                String value = cell.toString();
                columnWidths[i] = Math.max(columnWidths[i], value.length());
            }
        }
        
        // Header row with column names
        for (int i = 0; i < columnNames.size(); i++) {
            sb.append(String.format("%-" + (columnWidths[i] + 2) + "s", columnNames.get(i)));
        }
        sb.append("\n");
        
        // Separator line
        for (int width : columnWidths) {
            for (int i = 0; i < width + 2; i++) {
                sb.append("-");
            }
        }
        sb.append("\n");
        
        // Data rows
        for (List<Cell> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                sb.append(String.format("%-" + (columnWidths[i] + 2) + "s", row.get(i).toString()));
            }
            sb.append("\n");
        }
        
        return sb.toString();
    }
}