package cn.tenmg.flink.jobs.config.model;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class CreateTable extends SqlQuery {

    @XmlAttribute
    private String tableName;

    @XmlAttribute
    private String dataSource;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }
}
