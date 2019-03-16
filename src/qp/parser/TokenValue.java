package qp.parser;

/**
 * Defines the token used for parser and scanner.
 */
public class TokenValue {
    private String text;

    /**
     * Default Constructor.
     */
    public TokenValue() {
    }

    /**
     * Constructor of TokenValue.
     */
    public TokenValue(String text) {
        this.text = text;
    }

    /**
     * Getter of text.
     */
    public String text() {
        return text;
    }

    /**
     * @return the boolean value of text.
     */
    public Boolean toBoolean() {
        return Boolean.valueOf(text);
    }

    /**
     * @return the first character of text.
     */
    public Character toCharacter() {
        return text.charAt(0);
    }

    /**
     * @return the integer value of text.
     */
    public Integer toInteger() {
        if (text.startsWith("0x")) {
            return (int) Long.parseLong(text.substring(2), 16);
        } else {
            return Integer.valueOf(text, 10);
        }
    }
}
