public class FloatType implements DataType {
    @Override
    public boolean validate(String value) {
        if (value == null) {
            return false;
        }
        try {
            Float.parseFloat(value);
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
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse '" + value + "' as float", e);
        }
    }

    @Override
    public String toString() {
        return "FloatType";
    }
}