package qp.parser;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.Vector;

import org.junit.Test;

import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.SQLQuery;

/**
 * This is more like an integration test (parser together with scanner) rather than unit
 * test. We test it in this way because scanner & parser are tightly coupled.
 */
public class ParserTest {
    @Test
    public void selectStarWithoutWhere() throws Exception {
        SQLQuery query = parseString("SELECT * FROM customers");
        assertEquals(1, query.getFromList().size());
        assertEquals("customers", query.getFromList().elementAt(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
    }

    @Test
    public void selectOneAttributeWithoutWhere() throws Exception {
        SQLQuery query = parseString("SELECT customers.age FROM customers");
        assertEquals(1, query.getFromList().size());
        assertEquals("customers", query.getFromList().elementAt(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(1, query.getProjectList().size());
        assertAttributeName(query.getProjectList(), 0, "age");
        assertEquals(0, query.getNumJoin());
    }

    @Test
    public void selectMultiAttributesWithoutWhere() throws Exception {
        SQLQuery query = parseString("SELECT customers.age, customers.name FROM customers");
        assertEquals(1, query.getFromList().size());
        assertEquals("customers", query.getFromList().elementAt(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(2, query.getProjectList().size());
        assertAttributeName(query.getProjectList(), 0, "age");
        assertAttributeName(query.getProjectList(), 1, "name");
        assertEquals(0, query.getNumJoin());
    }

    @Test
    public void selectWithWhereEqual() throws Exception {
        SQLQuery query = parseString("SELECT customers.age FROM customers WHERE customers.size = \"100\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("customers", query.getFromList().elementAt(0));
        assertEquals(1, query.getSelectionList().size());
        assertSelectCondition(query.getSelectionList(), 0, "size", "100", Condition.EQUAL);
        assertEquals(1, query.getProjectList().size());
        assertAttributeName(query.getProjectList(), 0, "age");
        assertEquals(0, query.getNumJoin());
    }

    @Test
    public void selectWithWhereGreaterThan() throws Exception {
        SQLQuery query = parseString("SELECT customers.name FROM customers WHERE customers.age > \"25\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("customers", query.getFromList().elementAt(0));
        assertEquals(1, query.getSelectionList().size());
        assertSelectCondition(query.getSelectionList(), 0, "age", "25", Condition.GREATER_THAN);
        assertEquals(1, query.getProjectList().size());
        assertAttributeName(query.getProjectList(), 0, "name");
        assertEquals(0, query.getNumJoin());
    }

    private SQLQuery parseString(String input) throws Exception {
        StringReader reader = new StringReader(input);
        Scanner scanner = new Scanner(reader);
        parser p = new parser(scanner);
        p.parse();
        return p.getSQLQuery();
    }

    private void assertSelectCondition(Vector selectionList, int id, String leftAttrName, String rightValue, int operator) {
        Condition condition = (Condition) selectionList.elementAt(id);
        assertEquals(Condition.SELECT, condition.getCondType());
        assertEquals(leftAttrName, condition.getLeft().getColName());
        assertEquals(rightValue, condition.getRight());
        assertEquals(operator, condition.getOperator());
    }

    private void assertAttributeName(Vector projectList, int id, String expectedName) {
        Attribute attribute = (Attribute) projectList.elementAt(id);
        assertEquals(expectedName, attribute.getColName());
    }
}
