import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcedureManager {
    private static final List<String> SCHEMAS = Arrays.asList("J1_LDCX", "J1_DW", "J1_CXTJ");

    private int spaceNum;
    private char bracketL;
    private char bracketR;

    private Set<String> moduleParsedTables; // 整个模块被解析过的表，如解析过打印表[...]

    private Connection conn;

    public ProcedureManager(Connection conn) {
        this.spaceNum = 4;
        this.bracketL = '[';
        this.bracketR = ']';
        this.conn = conn;
    }

    public void setSpaceNum(int spaceNum) {

        this.spaceNum = spaceNum;
    }

    public void setBracketL(char bracketL) {

        this.bracketL = bracketL;
    }

    public void setBracketR(char bracketR) {

        this.bracketR = bracketR;
    }

    public void print(String highTable) {

        List<Procedure> ps = analyse(transform(highTable));
        if (ps.isEmpty()) {
            System.out.println("查无此表！");
        }
        else {
            for (Procedure p : ps) {
                System.out.println("TABLE_NAME: " + p.getTableComments());
                System.out.println("PROCEDURE: " + p.getName());
                System.out.println("PACKAGE: " + p.getPkg());
                System.out.println("SCHEMA: " + p.getSchema());
                System.out.println("UNIT_ID: " + p.getUnitId());
                System.out.println("PARENTUNIT_ID: " + p.getParentUnitId());
                System.out.println("UNIT_GROUP: " + p.getUnitGroup());
                tree(p.getTables());
            }
        }
    }

    public List<Procedure> analyse(String highTable) {
        List<Procedure> ps = query(highTable);

        for (Procedure p : ps) {
            moduleParsedTables = new HashSet<String>(); // 每个过程为一个单位
            p.setTables(findTables(p));
        }

        return ps;
    }

    private String findTables(Procedure procedure) {
        Set<String> levelParsedTables = new HashSet<String>(); // 同一递归层级被解析的表，避免同一层级重复表打印

        StringBuilder sb = new StringBuilder();
        String highTable = procedure.getHighTable();
        String unitId = procedure.getUnitId();
        sb.append(highTable)
                .append("(")
                .append(unitId)
                .append(")")
                .append(bracketL);

        Matcher m = Pattern
                .compile("(?i)\\b((?:ldm|d[wm][0-9])\\w+)\\b")
                .matcher(procedure.getSource());

        while (m.find()) {
            String t = transform(m.group(1));

            if (!levelParsedTables.contains(t)) {

                if (highTable.equals(t)) {
                    continue;
                }
                else if (t.startsWith("LDM")) {
                    sb.append(t).append(",");
                }
                else if (t.startsWith("DM") || t.startsWith("DW")) {

                    if (moduleParsedTables.contains(t)) {
                        sb.append(t)
                                .append(bracketL)
                                .append("...")
                                .append(bracketR)
                                .append(",");
                    }
                    else {
                        List<Procedure> ps = query(t);
                        if (!ps.isEmpty()) {

                            if (ps.size() == 1) {  // 只有一条记录才添加到moduleParsedTables，多条记录unitid不同
                                moduleParsedTables.add(t);
                                sb.append(findTables(ps.get(0))).append(",");
                            }
                            else {
                                for (Procedure p : ps) {
                                    if (isParent(p, procedure)) {
                                        sb.append(findTables(p)).append(",");
                                    }
                                }
                            }
                        }
                    }
                }
                else {
                    sb.append(t).append(",");
                }
            }
            levelParsedTables.add(t);
        }

        if (sb.indexOf(",") != -1) {
            sb.deleteCharAt(sb.length() - 1);
        }

        sb.append(bracketR);
        return sb.toString();
    }

    private List<Procedure> query(String highTable) {
        PreparedStatement statement = null;
        ResultSet result = null;

        String name = "P_" + highTable;

        List<Procedure> ps = new ArrayList<Procedure>();

        for (String s : SCHEMAS) {
            String sql = "SELECT UNIT_ID, UNIT_CODE, UNIT_GROUP, PARENTUNIT_ID FROM "
                    + s
                    + ".ETL_META_UNIT WHERE UNIT_NAME = ?";
            boolean isFounded = false;

            try {
                statement = conn.prepareStatement(sql);
                statement.setString(1, name);
                result = statement.executeQuery();

                while (result.next()) {
                    isFounded = true;
                    Procedure p = new Procedure();
                    p.setUnitId(transform(result.getString("UNIT_ID")));
                    p.setUnitGroup(transform(result.getString("UNIT_GROUP")));
                    p.setParentUnitId(transform(result.getString("PARENTUNIT_ID")));
                    p.setName(name);
                    p.setSchema(s);
                    p.setPkg(transform(result.getString("UNIT_CODE").split("\\.")[0]));
                    p.setHighTable(highTable);
                    String packageSource = getSource(p.getPkg());
                    String procedureSource = getSource(packageSource, p.getName());
                    p.setSource(procedureSource);
                    p.setTableComments(getTableComments(s, highTable));
                    ps.add(p);
                }

                if (isFounded) {
                    return ps;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                DBUtil.closeIfOpen(result);
                DBUtil.closeIfOpen(statement);
            }
        }

        return Collections.emptyList();
    }

    private boolean isParent(Procedure parent, Procedure son) {
        return parent.getUnitId().equals(son.getParentUnitId())
                || parent.getUnitGroup().equals(son.getUnitGroup());
    }

    private String getSource(String pkgSource, String procedureName) {
        Pattern p = Pattern.compile("(?i)(?s)procedure\\s+"
                + procedureName
                + "\\b(.+?)procedure\\s");

        Matcher m = p.matcher(pkgSource);

        String source = "";
        if (m.find()) {
            // 去掉注释再解析
            source = m.group(1)
                    .replaceAll("(?m)--.*$", "")
                    .replaceAll("(?s)/\\*(.*?)\\*/", "");
        }

        return source;
    }

    private String getSource(String pkg) {
        PreparedStatement statement = null;
        ResultSet result = null;

        String sql = "SELECT TEXT FROM DBA_SOURCE WHERE TYPE = 'PACKAGE BODY' AND NAME = ?";

        StringBuilder sb = new StringBuilder();
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, pkg);
            result = statement.executeQuery();

            while (result.next()) {
                sb.append(result.getString("TEXT"));
            }
            sb.append("procedure "); // 不加这句，包里最后一个存储过程或者包里只有一个过程，正则表达式sourceR匹配不到
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBUtil.closeIfOpen(result);
            DBUtil.closeIfOpen(statement);
        }
        return sb.toString();
    }

    private void tree(String tables) {
        int level = 0;
        for (int i = 0; i < tables.length(); i++) {
            char c = tables.charAt(i);

            if (c != ' ') {
                if (c == bracketL) {
                    level++;
                    System.out.print("-");
                    enter();
                    indent(level);
                }
                else if (c == bracketR) {
                    level--;
                    indent(level);
                }
                else if (c == ',') {
                    enter();
                    indent(level);
                }
                else {
                    System.out.print(String.valueOf(c));
                }
            }
        }
        enter();
        System.out.println("/");
    }

    private void indent(int level) {
        for (int i = 0; i < spaceNum * level; i++) {
            System.out.print(" ");
        }
    }

    private void enter() {

        System.out.println();
    }

    private String transform(String param) {
        if (param == null) {
            return "";
        }
        else {
            return param.trim().toUpperCase();
        }
    }

    private String getTableComments(String schema, String highTable) {
        PreparedStatement statement = null;
        ResultSet result = null;

        String sql = "SELECT COMMENTS FROM DBA_TAB_COMMENTS WHERE OWNER = ? AND TABLE_NAME = ?";
        String comments = ""; // 如果没有注释，设为空
        try {
            statement = conn.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, highTable);
            result = statement.executeQuery();

            while (result.next()) {
                comments = result.getString("COMMENTS");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBUtil.closeIfOpen(result);
            DBUtil.closeIfOpen(statement);
        }
        return comments;
    }
}