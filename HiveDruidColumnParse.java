package com.tcl.utils.lineage;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.hive.ast.HiveInsertStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.tcl.utils.ChkUtil;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * 基于Druid的Ast二次封装hive字段血缘类
 *
 * @author jinlong.gong
 * @date: 2022.10.09
 */
public class HiveDruidColumnParse {
    public static LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
    private static int i = -1;

    public LinkedHashMap<String, Object> setTenantParameter(String sql) {
        i = -1;
        map.clear();
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        SQLStatement statement = statementList.get(0);

        if (statement instanceof SQLSelectStatement) {
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
            processSelectBody(sqlSelectStatement.getSelect().getQuery());
            // System.out.println(map);
        }
        if (statement instanceof HiveInsertStatement) {
            HiveInsertStatement hiveInsertStatement = (HiveInsertStatement) statement;
            //with 解析
            SQLWithSubqueryClause sqlWithSubqueryClause = hiveInsertStatement.getWith();
            if(!ChkUtil.isEmpty(sqlWithSubqueryClause)){
                processWithSelectBody(sqlWithSubqueryClause);
            }
            SQLSelect sqlSelect=hiveInsertStatement.getQuery();
            if(sqlSelect.getQuery() instanceof SQLSelectQueryBlock){
                SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) hiveInsertStatement.getQuery().getQuery();
                processSelectBody(sqlSelectQueryBlock);
            } else if (sqlSelect.getQuery() instanceof SQLUnionQuery){
                SQLSubqueryTableSource subqueryTableSource = new SQLSubqueryTableSource(sqlSelect);
                SQLSelectQuery query = subqueryTableSource.getSelect().getQuery();
                map.put(i + "--alias", this.getAlias(subqueryTableSource));
                processSelectBody(query);
            }
        }
        if (statement instanceof SQLDeleteStatement) {
            /*processDelete((SQLDeleteStatement) statement);*/
        }

        return map;
    }

    public void processWithSelectBody(SQLWithSubqueryClause sqlWithSubqueryClause) {
        for (SQLWithSubqueryClause.Entry subWith : sqlWithSubqueryClause.getEntries()) {
            System.out.println(subWith);
            i++;
            SQLSubqueryTableSource subQueryTableSource = new SQLSubqueryTableSource(subWith.getSubQuery().clone(), subWith.getAlias());
            SQLSelectQuery query = subQueryTableSource.getSelect().getQuery();
            map.put(i + "--alias", this.getAlias(subQueryTableSource));
            processSelectBody(query);

        }
    }

    public void processSelectBody(SQLSelectQuery sqlSelectQuery) {
        i++;
        System.out.println(i);
        String tableName = null;
        if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            List<SQLSelectItem> selectList = sqlSelectQueryBlock.getSelectList();
            SQLTableSource table = sqlSelectQueryBlock.getFrom();
            SQLTableSource sqlTableSource = findFirstParent(sqlSelectQueryBlock, SQLTableSource.class);
            // 处理select 子查询
            selectList.forEach(sqlSelectItem -> {
                if (sqlSelectItem.getExpr() instanceof SQLQueryExpr) {
                    SQLQueryExpr expr = (SQLQueryExpr) sqlSelectItem.getExpr();
                    SQLSelectQuery query = expr.getSubQuery().getQuery();
                    processSelectBody(query);
                } else if (sqlSelectItem.getExpr() instanceof SQLExistsExpr) {
                    // 处理exists查询
                    SQLExistsExpr sqlExistsExpr = (SQLExistsExpr) sqlSelectItem.getExpr();
                    SQLSelectQuery query = sqlExistsExpr.getSubQuery().getQuery();
                    processSelectBody(query);
                }
            });
            map.put(i + "--selectList", selectList);
            if (table instanceof SQLExprTableSource) {
                SQLExpr where = sqlSelectQueryBlock.getWhere();
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) table;
                tableName = sqlExprTableSource.getExpr().toString();
                String alias = null;
                if (sqlTableSource != null) {
                    alias = sqlTableSource.getAlias();
                    // System.out.println(alias);
                }
                map.put(i + "--tableName", tableName);
                map.put(i + "--alias", alias);
            } else if (table instanceof SQLJoinTableSource) {
                SQLJoinTableSource joinTable = (SQLJoinTableSource) table;
                SQLTableSource left = joinTable.getLeft();
                //获得0节点的别名
                String alias = this.getAlias(left);
                map.put(i + "--alias", alias);
                //获取on
                SQLExpr condition = joinTable.getCondition();

                tableName = this.joinCondition(left);
                if (tableName != null) {
                    map.put(i + "--tableName", tableName);

                }
                SQLTableSource right = joinTable.getRight();

                if (right instanceof SQLExprTableSource) {
                    SQLTableSource subQueryTable = right;
                    i++;
                    map.put(i + "--alias", this.getAlias(subQueryTable));

                    tableName = this.joinCondition(right);
                    if (tableName != null) {
                        map.put(i + "--tableName", tableName);
                    }
                    map.put(i + "--alias", this.getAlias(right));
                } else {
                    map.put(i + "--alias", this.getAlias(right));
                    tableName = this.joinCondition(right);
                    if (tableName != null) {
                        map.put(i + "--tableName", tableName);
                    }

                }


/*                SQLExpr tenantCondition = getTenantCondition(tableName, right.getAlias(), joinTable.getCondition());
                if (tenantCondition != null) {
                    joinTable.addCondition(tenantCondition);
                }*/
                // 构造新的 where
                SQLExpr where = sqlSelectQueryBlock.getWhere();

                if (sqlSelectQueryBlock.getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlSelectQueryBlock.getFrom();
                    tableName = sqlExprTableSource.getExpr().toString();
                }
                if (left instanceof SQLJoinTableSource) {
                    SQLJoinTableSource joinTableSource = (SQLJoinTableSource) left;
                    if (joinTableSource.getLeft() instanceof SQLExprTableSource) {
                        SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) joinTableSource.getLeft();
                        tableName = sqlExprTableSource.getExpr().toString();
                    }

                }

            } else if (table instanceof SQLSubqueryTableSource) {
                // 子查询作为表
                SQLSubqueryTableSource subQueryTable = (SQLSubqueryTableSource) table;
                SQLSelectQuery query = subQueryTable.getSelect().getQuery();
                map.put(i + "--alias", this.getAlias(subQueryTable));
                processSelectBody(query);
            } else if (table instanceof SQLUnionQueryTableSource) {
                SQLUnionQueryTableSource unionQueryTableSource = (SQLUnionQueryTableSource) table;
                SQLUnionQuery unionQuery = unionQueryTableSource.getUnion();
                List<SQLSelectQuery> relations = unionQuery.getRelations();
                for (SQLSelectQuery selectQuery : relations) {
                    if (selectQuery instanceof SQLSelectQueryBlock) {
                        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) selectQuery;
                        processSelectBody(queryBlock);
                    } else {
                        SQLUnionQuery unionSelectQuery = (SQLUnionQuery) selectQuery;
                        unionSelectQuery.getRelations().forEach(this::processSelectBody);
                    }
                    map.put(i + "--Operator", unionQuery.getOperator());

                }
            }
        } else if (sqlSelectQuery instanceof SQLUnionQuery) {
            // 处理union的查询语句
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            map.put(i + "--Operator", sqlUnionQuery.getOperator());
            sqlUnionQuery.getRelations().forEach(this::processSelectBody);
        } else if (sqlSelectQuery instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlSelectQuery;
            tableName = sqlExprTableSource.getExpr().toString();
            map.put(i + "--alias", this.getAlias(sqlExprTableSource));

        }
    }

    /**
     * 多表关联查询 on 添加字段
     *
     * @param sqlTableSource
     */
    private String joinCondition(SQLTableSource sqlTableSource) {
        String tableName = null;
        if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource left = sqlJoinTableSource.getLeft();
            SQLTableSource right = sqlJoinTableSource.getRight();

            if (right instanceof SQLExprTableSource) {
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) right;
                tableName = sqlExprTableSource.getExpr().toString();

            } else if (left instanceof SQLExprTableSource) {
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) left;
                tableName = sqlExprTableSource.getExpr().toString();
            }

            this.joinCondition(left);
            if (right != null) {
                this.joinCondition(right);
            }
            SQLExpr condition = sqlJoinTableSource.getCondition();
            map.put(i + "--condition", condition);
        } else if (sqlTableSource instanceof SQLSubqueryTableSource) {
            // 子查询作为表
            SQLSubqueryTableSource subQueryTable = (SQLSubqueryTableSource) sqlTableSource;
            SQLSelectQuery query = subQueryTable.getSelect().getQuery();
            if (map.get(i + "--alias") == null) {
                map.put(i + "--alias", this.getAlias(subQueryTable));
            }
            SQLJoinTableSource subQueryTable2 = (SQLJoinTableSource) sqlTableSource.getParent();
            SQLExpr condition = subQueryTable2.getCondition();
            processSelectBody(query);
            if (map.get(i + "--condition") == null) {
                map.put(i + "--condition", condition);
            }
        } else if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            tableName = sqlExprTableSource.getExpr().toString();
            SQLJoinTableSource subQueryTable2 = (SQLJoinTableSource) sqlTableSource.getParent();
            SQLExpr condition = subQueryTable2.getCondition();
            if (map.get(i + "--condition") == null) {
                map.put(i + "--condition", condition);
            }
        }
        return tableName;

    }

    private static <T extends SQLObject> T findFirstParent(SQLObject sqlObject, Class<T> tClass) {
        SQLObject currParent = sqlObject.getParent();
        while (currParent != null) {
            if (tClass.isAssignableFrom(currParent.getClass())) {
                return (T) currParent;
            }
            currParent = currParent.getParent();
        }
        return null;
    }

    /**
     * 获取别名
     * 若果是多表查询获取第一个表的别名
     *
     * @param sqlTableSource
     * @return
     */
    private String getAlias(SQLTableSource sqlTableSource) {
        if (sqlTableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            if (sqlJoinTableSource.getLeft() instanceof SQLJoinTableSource) {
                return getAlias(sqlJoinTableSource.getLeft());
            } else if (sqlJoinTableSource.getLeft() instanceof SQLExprTableSource) {
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlJoinTableSource.getLeft();
                return sqlExprTableSource.getAlias();
            } else {
                return sqlJoinTableSource.getAlias();
            }
        } else if (sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            return sqlExprTableSource.getAlias();
        } else if (sqlTableSource != null) {
            return sqlTableSource.getAlias();
        } else {
            return null;
        }
    }
}
