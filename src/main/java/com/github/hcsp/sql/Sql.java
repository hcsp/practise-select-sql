
package com.github.hcsp.sql;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Sql {
// 用户表：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
// | 2  | lisi     | tel2 | shanghai |
// +----+----------+------+----------+
// | 3  | wangwu   | tel3 | shanghai |
// +----+----------+------+----------+
// | 4  | zhangsan | tel4 | shenzhen |
// +----+----------+------+----------+
// 商品表：
// +----+--------+-------+
// | ID | NAME   | PRICE |
// +----+--------+-------+
// | 1  | goods1 | 10    |
// +----+--------+-------+
// | 2  | goods2 | 20    |
// +----+--------+-------+
// | 3  | goods3 | 30    |
// +----+--------+-------+
// | 4  | goods4 | 40    |
// +----+--------+-------+
// | 5  | goods5 | 50    |
// +----+--------+-------+
// 订单表：
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | ID(订单ID) | USER_ID(用户ID) | GOODS_ID(商品ID) | GOODS_NUM(商品数量) | GOODS_PRICE(下单时的商品单价)        |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 1          | 1               | 1                | 5                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 2          | 2               | 1                | 1                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 3          | 2               | 1                | 2                   | 10                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 4          | 4               | 2                | 4                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 5          | 4               | 2                | 100                 | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 6          | 4               | 3                | 1                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 7          | 5               | 4                | 1                   | 20                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+
// | 8          | 5               | 6                | 1                   | 60                            |
// +------------+-----------------+------------------+---------------------+-------------------------------+

    // 用户信息
    public static class User {
        Integer id;
        String name;
        String tel;
        String address;

        @Override
        public String toString() {
            return "User{" + "id=" + id + ", name='" + name + '\'' + ", tel='" + tel + '\'' + ", address='" + address + '\'' + '}';
        }
    }

    /**
     * 题目1：
     * 查询有多少所有用户曾经买过指定的商品
     *
     * @param goodsId 指定的商品ID
     * @return 有多少用户买过这个商品
     */
// 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        String sql = "select count(distinct USER_ID) from \"ORDER\" where GOODS_ID  = ?";
        Object[] params = new Object[]{goodsId};
        ResultSet rs = executeQuery(databaseConnection, sql, params);
        return rs.next() ? rs.getInt(1) : 0;
    }

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param pageNum  第几页，从1开始
     * @param pageSize 每页有多少个元素
     * @return 指定页中的用户
     */
// 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws Exception {

        String sql = "select id, name, tel, address\n" +
                "from USER\n" +
                "order by ID desc\n" +
                "limit ? offset ?";
        Object[] params = new Object[]{pageSize, (pageNum - 1) * pageSize};

        return executeQueryAndGetList(databaseConnection, sql, params, new User());

    }

    // 商品及其营收
    public static class GoodsAndGmv {
        Integer goodsId; // 商品ID
        String goodsName; // 商品名
        BigDecimal gmv; // 商品的所有销售额

        @Override
        public String toString() {
            return "GoodsAndGmv{" + "goodsId=" + goodsId + ", goodsName='" + goodsName + '\'' + ", gmv=" + gmv + '}';
        }
    }

    /**
     * 题目3：
     * 查询所有的商品及其销售额，按照销售额从大到小排序
     */
// 预期的结果应该如图所示
//  +----+--------+------+
//  | ID | NAME   | GMV  |
//  +----+--------+------+
//  | 2  | goods2 | 2080 |
//  +----+--------+------+
//  | 1  | goods1 | 80   |
//  +----+--------+------+
//  | 4  | goods4 | 20   |
//  +----+--------+------+
//  | 3  | goods3 | 20   |
//  +----+--------+------+
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws Exception {

        String sql = "select GOODS_ID as goodsId, max(NAME) as goodsName, sum(GOODS_PRICE * GOODS_NUM) as gmv\n" +
                "from \"ORDER\"\n" +
                "         join GOODS\n" +
                "              on \"ORDER\".GOODS_ID = GOODS.ID\n" +
                "group by GOODS_ID\n" +
                "order by GMV desc";
        return executeQueryAndGetList(databaseConnection, sql, null, new GoodsAndGmv());
    }


    // 订单详细信息
    public static class Order {
        Integer id; // 订单ID
        String userName; // 用户名
        String goodsName; // 商品名
        BigDecimal totalPrice; // 订单总金额

        @Override
        public String toString() {
            return "Order{" + "id=" + id + ", userName='" + userName + '\'' + ", goodsName='" + goodsName + '\'' + ", totalPrice=" + totalPrice + '}';
        }
    }

    /**
     * 题目4：
     * 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     */
// 预期的结果为：
// +----------+-----------+------------+-------------+
// | ORDER_ID | USER_NAME | GOODS_NAME | TOTAL_PRICE |
// +----------+-----------+------------+-------------+
// | 1        | zhangsan  | goods1     | 50          |
// +----------+-----------+------------+-------------+
// | 2        | lisi      | goods1     | 10          |
// +----------+-----------+------------+-------------+
// | 3        | wangwu    | goods1     | 20          |
// +----------+-----------+------------+-------------+
// | 4        | zhangsan  | goods2     | 80          |
// +----------+-----------+------------+-------------+
// | 5        | zhangsan  | goods2     | 2000        |
// +----------+-----------+------------+-------------+
// | 6        | zhangsan  | goods3     | 20          |
// +----------+-----------+------------+-------------+
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws Exception {
        String sql = "select \"ORDER\".id as orderId, user.NAME as userName, GOODS.NAME as goodsName, GOODS_NUM * GOODS_PRICE as totalPrice\n" +
                "from \"ORDER\"\n" +
                "         join USER on \"ORDER\".USER_ID = USER.ID\n" +
                "         join GOODS on \"ORDER\".GOODS_ID = GOODS.ID";
        return executeQueryAndGetList(databaseConnection, sql, null, new Order());
    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     */
// 预期的结果为：
// +----------+-----------+------------+-------------+
// | ORDER_ID | USER_NAME | GOODS_NAME | TOTAL_PRICE |
// +----------+-----------+------------+-------------+
// | 1        | zhangsan  | goods1     | 50          |
// +----------+-----------+------------+-------------+
// | 2        | lisi      | goods1     | 10          |
// +----------+-----------+------------+-------------+
// | 3        | wangwu    | goods1     | 20          |
// +----------+-----------+------------+-------------+
// | 4        | zhangsan  | goods2     | 80          |
// +----------+-----------+------------+-------------+
// | 5        | zhangsan  | goods2     | 2000        |
// +----------+-----------+------------+-------------+
// | 6        | zhangsan  | goods3     | 20          |
// +----------+-----------+------------+-------------+
// | 7        | NULL      | goods4     | 20          |
// +----------+-----------+------------+-------------+
// | 8        | NULL      | NULL       | 60          |
// +----------+-----------+------------+-------------+
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws Exception {
        String sql = "select \"ORDER\".id as orderId, user.NAME as userName, GOODS.NAME as goodsName, GOODS_NUM * GOODS_PRICE as totalPrice\n" +
                "from \"ORDER\"\n" +
                "         left join USER on \"ORDER\".USER_ID = USER.ID\n" +
                "         left join GOODS on \"ORDER\".GOODS_ID = GOODS.ID";
        return executeQueryAndGetList(databaseConnection, sql, null, new Order());
    }

    // 注意，运行这个方法之前，请先运行mvn initialize把测试数据灌入数据库
    public static void main(String[] args) throws Exception {
        File projectDir = new File(System.getProperty("basedir", System.getProperty("user.dir")));
        String jdbcUrl = "jdbc:h2:file:" + new File(projectDir, "target/test").getAbsolutePath();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "root", "Jxi1Oxc92qSj")) {
            System.out.println(countUsersWhoHaveBoughtGoods(connection, 1));
            System.out.println(getUsersByPageOrderedByIdDesc(connection, 2, 3));
            System.out.println(getGoodsAndGmv(connection));
            System.out.println(getInnerJoinOrders(connection));
            System.out.println(getLeftJoinOrders(connection));
        }
    }

    private static <T> List<T> executeQueryAndGetList(Connection databaseConnection, String sql, Object[] params, T t) throws Exception {
        ResultSet rs = executeQuery(databaseConnection, sql, params);
        return convertResultSetToList(rs, t);
    }

    private static ResultSet executeQuery(Connection databaseConnection, String sql, Object[] params) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement(sql);

        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
        }

        return statement.executeQuery();
    }

    private static <T> List<T> convertResultSetToList(ResultSet rs, T t) throws Exception {
        List<T> list = new ArrayList<>();
        Class<T> c = (Class<T>) t.getClass();
        Field[] fs = c.getDeclaredFields();

        if (rs != null) {
            while (rs.next()) {
                t = c.getDeclaredConstructor().newInstance();
                for (Field field : fs) {
                    Field f = t.getClass().getDeclaredField(field.getName());
                    f.setAccessible(true);
                    String typeName = f.getType().getName();
                    String filedName = field.getName();

                    if (typeName.equals(String.class.getName())) {
                        f.set(t, rs.getString(filedName));
                    } else if (typeName.equals(Integer.class.getName())
                            || typeName.equals(int.class.getName())) {
                        f.set(t, rs.getInt(filedName));
                    } else if (typeName.equals(BigDecimal.class.getName())) {
                        f.set(t, rs.getBigDecimal(filedName));
                    } else {
                        //to do 匹配别的类型
                    }
                }
                list.add(t);
            }
        }
        return list;
    }
}
