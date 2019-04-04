package qp.utils;

import java.io.Serializable;
import java.util.Objects;

/**
 * Defines some metadata about an attribute (i.e., a column).
 */
public class Attribute implements Serializable {
    // Data types of an attribute.
    public static final int INT = 1;
    public static final int STRING = 2;
    public static final int REAL = 3;
    public static final int TIME = 4;

    // Key types of an attribute.
    public static final int PK = 1; // primary key
    public static final int FK = 2; // Foreign key

    // The name of the table to which this attribute belongs.
    private String tableName;
    // The column name of this attribute.
    private String colName;
    // The getData type of this attribute.
    private int type;
    // The key type of this attribute.
    private int key = -1;
    // The size (in bytes) of this attribute.
    private int attrSize;

    /**
     * Creates a new attribute.
     *
     * @param tbl is the table name.
     * @param col is the column name.
     */
    public Attribute(String tbl, String col) {
        tableName = tbl;
        colName = col;
    }

    /**
     * Creates a new attribute.
     *
     * @param tbl is the table name.
     * @param col is the column name.
     * @param typ is the getData type.
     */
    public Attribute(String tbl, String col, int typ) {
        tableName = tbl;
        colName = col;
        type = typ;
    }

    /**
     * Creates a new attribute.
     *
     * @param tbl is the table name.
     * @param col is the column name.
     * @param typ is the getData type.
     * @param keyType is the key type.
     */
    public Attribute(String tbl, String col, int typ, int keyType) {
        tableName = tbl;
        colName = col;
        type = typ;
        key = keyType;
    }

    /**
     * Creates a new attribute.
     *
     * @param tbl is the table name.
     * @param col is the column name.
     * @param typ is the getData type.
     * @param keyType is the key type.
     * @param size is the size (in bytes).
     */
    public Attribute(String tbl, String col, int typ, int keyType, int size) {
        tableName = tbl;
        colName = col;
        type = typ;
        key = keyType;
        attrSize = size;
    }

    /**
     * Setter for the attribute size.
     *
     * @param size is the size (in bytes).
     */
    public void setAttrSize(int size) {
        attrSize = size;
    }

    /**
     * Getter for the attribute size.
     *
     * @return the size (in bytes).
     */
    public int getAttrSize() {
        return attrSize;
    }

    /**
     * Setter for key type.
     *
     * @param kt is the key type.
     */
    public void setKeyType(int kt) {
        key = kt;
    }

    /**
     * Getter for key type.
     *
     * @return the key type.
     */
    public int getKeyType() {
        return key;
    }

    /**
     * @return whether this attribute is a primary key.
     */
    public boolean isPrimaryKey() {
        return key == PK;
    }

    /**
     * @return whether this attribute is a foreign key.
     */
    public boolean isForeignKey() {
        return key == FK;
    }

    /**
     * Setter for table name.
     *
     * @param tab is the table name.
     */
    public void setTabName(String tab) {
        tableName = tab;
    }

    /**
     * Getter for table name.
     *
     * @return the table name.
     */
    public String getTabName() {
        return tableName;
    }

    /**
     * Setter for column name.
     *
     * @param col is the column name.
     */
    public void setColName(String col) {
        colName = col;
    }

    /**
     * Getter for column name.
     *
     * @return the column name.
     */
    public String getColName() {
        return colName;
    }

    /**
     * Setter for getData type.
     *
     * @param typ is the getData type.
     */
    public void setType(int typ) {
        type = typ;
    }

    /**
     * Getter for getData type.
     *
     * @return the getData type.
     */
    public int getType() {
        return type;
    }

    /**
     * Checks the equality with another object.
     *
     * @param other is the other object to be compared with.
     * @return true if the other object is also of {@link Attribute} type and has the same table name & column name.
     */
    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof Attribute) {
            Attribute otherAttr = (Attribute) other;
            return tableName.equals(otherAttr.tableName) && colName.equals(otherAttr.colName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, colName);
    }

    /**
     * Creates another copy of this attribute.
     *
     * @return the newly created copy of this attribute.
     */
    @Override
    public Object clone() {
        String newTableName = tableName;
        String newColName = colName;
        Attribute newAttr = new Attribute(newTableName, newColName);
        newAttr.setType(type);
        newAttr.setKeyType(key);
        newAttr.setAttrSize(attrSize);
        return newAttr;
    }
}
