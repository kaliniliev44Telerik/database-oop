public class Cell {
    private DataType type;
    private String value;
    private boolean isNull;

    public Cell(DataType type, String value) {
        this.type = type;
        this.value = value;
        this.isNull = (value == null);
    }
    
    public Cell(DataType type) {
        this.type = type;
        this.value = null;
        this.isNull = true;
    }
    
    public DataType getType() {
        return type;
    }
    
    public String getValue() {
        return value;
    }
    
    public boolean isNull() {
        return isNull;
    }
    
    public void setValue(String value) {
        if (value == null) {
            this.value = null;
            this.isNull = true;
            return;
        }
        
        if (type.validate(value)) {
            this.value = value;
            this.isNull = false;
        } else {
            throw new IllegalArgumentException("Value '" + value + "' is not valid for type " + type);
        }
    }
    
    public Object getParsedValue() {
        if (isNull) {
            return null;
        }
        return type.parse(value);
    }
    
    @Override
    public String toString() {
        if (isNull) {
            return "NULL";
        }
        return value;
    }
}