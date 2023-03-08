package test.java.run.chargpt;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.hive.ast.HiveInsertStatement;
import com.alibaba.druid.sql.dialect.hive.parser.HiveStatementParser;
import com.alibaba.druid.sql.dialect.hive.visitor.HiveSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.*;

;
;

public class SQLParserExample2 {

    public static void main(String[] args) {
        String sql = "INSERT OVERWRITE TABLE dw.agg_iot_device_error partition(date_type='D',date_id='20220621') select t1.device_category_code as category_code, t1.category_cn_name, t1.device_protocol as protocol, t1.device_product_key as product_key, t1.product_cn_name, t1.device_type, t1.error_code, t1.error_type, t1.device_country as country_cname, t1.device_province as province_cname, t1.device_city as city_cname, t1.date_type_1, t1.date_id_1, t1.device_id, t1.error_times from( SELECT case when nvl(e.category_code,'')='' then '未知' else e.category_code end device_category_code ,case when b.category_cn_name is null then '未知' else b.category_cn_name end category_cn_name ,case when nvl(e.protocol ,'')='' then '未知' else e.protocol end device_protocol ,case when nvl(e.product_key,'')='' then '未知' else e.product_key end device_product_key ,case when c.product_cn_name is not null then c.product_cn_name end as product_cn_name ,case when nvl(e.device_type,'')='' then '未知' else e.device_type end as device_type ,a.error_code ,nvl(case when a.api_code=55 then a.description else a.fault_type end,'未知') as error_type ,case when nvl(e.country,'') in ('','N/A','null') then '未知' else e.country end as device_country ,case when nvl(e.province,'') in ('','N/A','null') then '未知' else e.province end as device_province ,case when nvl(e.city,'') in ('','N/A','null') then '未知' else e.city end as device_city ,'D' date_type_1 ,'20220621' date_id_1 ,a.device_id ,COUNT(1) as error_times FROM( select a1.* from dw.fact_iot_device_error_log a1 left join (select distinct device_id from dw.dim_iot_test_user_device_list)b on a1.device_id=b.device_id where year||month||day='20220621' and b.device_id is null )a left join (select device_id,category_code,product_key,device_type,protocol,country,province,city from dw.dim_device_register ) e on a.device_id = e.device_id left join (select category_code,max(category_cn_name) category_cn_name from dw.dict_iot_device_category_info group by category_code)b on e.category_code = b.category_code left join (select product_key,max(product_cn_name) product_cn_name from dw.dict_iot_device_category_info group by product_key)c on e.product_key = c.product_key GROUP BY e.category_code , b.category_cn_name , e.protocol , e.product_key , c.product_cn_name , e.device_type , case when a.api_code=55 then a.description else a.fault_type end , a.error_code , e.country , e.province , e.city , a.device_id)t1 left join( select device_id from dw.fact_iot_device_error_log a where year||month||day='20220621' group by device_id having count(distinct error_code) >= 20 )t2 on t1.device_id=t2.device_id where t2.device_id is null; ";

        // 解析SQL语句
        HiveStatementParser parser = new HiveStatementParser(sql);
        SQLStatement statement = parser.parseStatement();

        if (statement instanceof HiveInsertStatement) {
            HiveInsertStatement hiveInsertStatement = (HiveInsertStatement) statement;
            HiveSchemaStatVisitor visitor = new HiveSchemaStatVisitor();
            hiveInsertStatement.accept(visitor);
            SQLSelect sqlSelect=hiveInsertStatement.getQuery();
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) sqlSelect.getQuery();
       /* }

        if (statement instanceof SQLSelectStatement) {*/
            /*SQLSelectStatement selectStatement = (SQLSelectStatement) statement;
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) selectStatement.getSelect().getQuery();*/

            // 遍历语法树，获取所有表
            List<SQLTableSource> tableSources = new ArrayList<>();
            queryBlock.accept(new TableSourceVisitor(tableSources));
            for (SQLTableSource tableSource : tableSources) {
                System.out.println("Table: " + tableSource.toString());
            }
            Set<String> joinKeys = new HashSet<>();
            queryBlock.accept(new JoinKeyVisitor(joinKeys));
            for (String joinKey : joinKeys) {
                System.out.println("Join Key: " + joinKey);
            }
            // 遍历语法树，获取所有别名
            Map<String, String> aliases = new HashMap<>();
            queryBlock.accept(new AliasVisitor(aliases));
            for (Map.Entry<String, String> entry : aliases.entrySet()) {
                System.out.println("Alias: " + entry.getKey() + " -> " + entry.getValue());
            }

            // 获取查询语句的层级别
            int level = 1;
            SQLObject parent = queryBlock.getParent();
            while (parent != null) {
                level++;
                parent = parent.getParent();
            }
            System.out.println("Query Level: " + level);

            // 遍历语法树，获取各SQL之间的关联关系
            Set<String> dependencies = new HashSet<>();
            statement.accept(new DependencyVisitor(dependencies));
            for (String dependency : dependencies) {
                System.out.println("Dependency: " + dependency);
            }

            // 输出结果
            Map<String, Object> result = new HashMap<>();
            result.put("tables", tableSources);
            result.put("join_keys", joinKeys);
            result.put("aliases", aliases);
            result.put("query_level", level);
            //result.put("dependencies", dependencies);
            String json = JSON.toJSONString(result, SerializerFeature.PrettyFormat);
            System.out.println(json);
        }
    }

    // 遍历语法树，获取所有表
    private static class TableSourceVisitor extends SQLASTVisitorAdapter {
        private List<SQLTableSource> tableSources;

        public TableSourceVisitor(List<SQLTableSource> tableSources) {
            this.tableSources = tableSources;
        }

        @Override
        public boolean visit(SQLExprTableSource x) {
            tableSources.add(x);
            return true;
        }

        @Override
        public boolean visit(SQLJoinTableSource x) {
            x.getLeft().accept(this);
            x.getRight().accept(this);
            return false;
        }

        @Override
        public boolean visit(SQLSubqueryTableSource x) {
            x.getSelect().getQuery().accept(this);
            return false;
        }
    }

    // 遍历语法树，获取所有关联键
    private static class JoinKeyVisitor extends SQLASTVisitorAdapter {
        private Set<String> joinKeys;

        public JoinKeyVisitor(Set<String> joinKeys) {
            this.joinKeys = joinKeys;
        }

        @Override
        public boolean visit(SQLBinaryOpExpr x) {
            if (x.getOperator() == SQLBinaryOperator.Equality) {
                SQLExpr left = x.getLeft();
                SQLExpr right = x.getRight();
                if (left instanceof SQLPropertyExpr && right instanceof SQLPropertyExpr) {
                    SQLPropertyExpr leftProp = (SQLPropertyExpr) left;
                    SQLPropertyExpr rightProp = (SQLPropertyExpr) right;
                    if (leftProp.getOwnerName().equalsIgnoreCase(rightProp.getOwnerName())) {
                        joinKeys.add(leftProp.getOwnerName() + "." + leftProp.getName());
                    } else {
                        joinKeys.add(leftProp.getOwnerName() + "." + leftProp.getName() + " <-> " +
                                rightProp.getOwnerName() + "." + rightProp.getName());
                    }
                }
            }
            return true;
        }
    }

    // 遍历语法树，获取所有别名
    private static class AliasVisitor extends SQLASTVisitorAdapter {
        private Map<String, String> aliases;

        public AliasVisitor(Map<String, String> aliases) {
            this.aliases = aliases;
        }

        @Override
        public boolean visit(SQLSelectItem x) {
            SQLExpr expr = x.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                SQLPropertyExpr propExpr = (SQLPropertyExpr) expr;
                String alias = x.getAlias();
                if (alias != null) {

                    //aliases.put(alias, propExpr.getFullName());
                    aliases.put(alias, propExpr.toString());
                }
            }
            return true;
        }
    }

    // 遍历语法树，获取各SQL之间的关联关系
    private static class DependencyVisitor extends SQLASTVisitorAdapter {
        private Set<String> dependencies;

        public DependencyVisitor(Set<String> dependencies) {
            this.dependencies = dependencies;
        }

        @Override
        public boolean visit(SQLSelectStatement x) {
            dependencies.add(x.toString());
            return true;
        }
    }
}