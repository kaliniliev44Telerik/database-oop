public class StringType implements DataType {
    @Override
    public boolean validate(String value) {
        return true;
    }
    
    @Override
    public Object parse(String value) {
        return value;
    }
    
    @Override
    public String toString() {
        return "StringType";
    }
    
    public static String escapeString(String value) {
        if (value == null) {
            return "NULL";
        }
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
    
    public static String unescapeString(String value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        

        if (value.length() < 2 || !value.startsWith("\"") || !value.endsWith("\"")) {
            throw new IllegalArgumentException("Invalid string format: " + value);
        }
        
        String content = value.substring(1, value.length() - 1);
        StringBuilder result = new StringBuilder();
        
        for (int i = 0; i < content.length(); i++) {
            if (content.charAt(i) == '\\' && i < content.length() - 1) {
                char next = content.charAt(i + 1);
                if (next == '\\' || next == '"') {
                    result.append(next);
                    i++; 
                } else {
                    result.append('\\');
                }
            } else {
                result.append(content.charAt(i));
            }
        }
        
        return result.toString();
    }
}