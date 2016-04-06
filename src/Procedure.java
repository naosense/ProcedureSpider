public class Procedure {
    private String unitId;
    private String unitGroup;
    private String parentUnitId;
    private String name; // 过程名
    private String schema;
    private String pkg;
    private String highTable; // 过程对应高阶表
    private String tables; // 高阶表基础表
    private String source; // 过程的源码
    private String tableComments; // 表的注释，大部分是指表的中文名

    public String getUnitId() {

        return unitId;
    }

    public void setUnitId(String unitId) {

        this.unitId = unitId;
    }

    public String getUnitGroup() {

        return unitGroup;
    }

    public void setUnitGroup(String unitGroup) {

        this.unitGroup = unitGroup;
    }

    public String getParentUnitId() {

        return parentUnitId;
    }

    public void setParentUnitId(String parentUnitId) {

        this.parentUnitId = parentUnitId;
    }

    public String getName() {

        return name;
    }

    public void setName(String name) {

        this.name = name;
    }

    public String getSchema() {

        return schema;
    }

    public void setSchema(String schema) {

        this.schema = schema;
    }

    public String getPkg() {

        return pkg;
    }

    public void setPkg(String pkg) {

        this.pkg = pkg;
    }

    public String getHighTable() {

        return highTable;
    }

    public void setHighTable(String highTable) {

        this.highTable = highTable;
    }

    public String getTables() {

        return tables;
    }

    public void setTables(String tables) {

        this.tables = tables;
    }

    public String getSource() {

        return source;
    }

    public void setSource(String source) {

        this.source = source;
    }

    public String getTableComments() {
        return tableComments;
    }

    public void setTableComments(String tableComments) {
        this.tableComments = tableComments;
    }
}
