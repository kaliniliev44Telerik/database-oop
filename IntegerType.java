public class IntegerType implements DataType {
    @Override
    public boolean validate(String value) {
        if (value == null) {
            return false;
        }
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    @Override
    public Object parse(String value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("Cannot parse null value");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse '" + value + "' as integer", e);
        }
    }

    @Override
    public String toString() {
        return "IntegerType";
    }
}