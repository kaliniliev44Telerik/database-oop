public interface DataType {
    boolean validate(String value);
    Object parse(String value) throws IllegalArgumentException;
    String toString();
}