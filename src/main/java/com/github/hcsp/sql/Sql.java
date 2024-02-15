
package com.github.hcsp.sql;
import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
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
     * @param databaseConnection 数据库连接
     * @param goodsId 指定的商品ID
     * @return 有多少用户买过这个商品
     */
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("SELECT COUNT(DISTINCT USER_ID) FROM `ORDER` WHERE GOODS_ID = " + goodsId);
        resultSet.next();
        return resultSet.getInt(1);
    }

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     * @param databaseConnection 数据库连接
     * @param pageNum  第几页，从1开始
     * @param pageSize 每页有多少个元素
     * @return 指定页中的用户
     */

    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("SELECT * FROM USER ORDER BY ID DESC LIMIT " + pageSize + " OFFSET " + (pageNum - 1) * pageSize);
        ArrayList<User> users = new ArrayList<>();
        while (resultSet.next()) {
            users.add(new User() {{
                id = resultSet.getInt("ID");
                name = resultSet.getString("NAME");
                tel = resultSet.getString("TEL");
                address = resultSet.getString("ADDRESS");
            }});
        }
        return users;
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
     * @param databaseConnection
     * @return List<GoodsAndGmv>
     * @throws SQLException
     */
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("SELECT GOODS_ID, NAME, SUM(GOODS_NUM * GOODS_PRICE) AS GMV FROM \"ORDER\", GOODS WHERE \"ORDER\".GOODS_ID = GOODS.ID group by GOODS_ID ORDER BY GMV DESC\n");
        ArrayList<GoodsAndGmv> goodsAndGmvs = new ArrayList<>();
        while (resultSet.next()) {
            goodsAndGmvs.add(new GoodsAndGmv() {{
                goodsId = resultSet.getInt("GOODS_ID");
                goodsName = resultSet.getString("NAME");
                gmv = resultSet.getBigDecimal("GMV");
            }});
        }
        return goodsAndGmvs;
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
     * @param databaseConnection
     * @return List<Order>
     * @throws SQLException
     */

    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("SELECT \"ORDER\".ID AS ORDER_ID, USER.NAME AS USER_NAME, GOODS.NAME AS GOODS_NAME, SUM(\"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM) AS TOTAL_PRICE FROM \"ORDER\" INNER JOIN USER ON \"ORDER\".USER_ID = USER.ID INNER JOIN GOODS ON \"ORDER\".GOODS_ID = GOODS.ID GROUP BY \"ORDER\".ID, USER.NAME, GOODS.NAME");
        ArrayList<Order> orders = new ArrayList<>();
        while (resultSet.next()) {
            orders.add(new Order() {{
                id = resultSet.getInt("ORDER_ID");
                userName = resultSet.getString("USER_NAME");
                goodsName = resultSet.getString("GOODS_NAME");
                totalPrice = resultSet.getBigDecimal("TOTAL_PRICE");
            }});
        }
        return orders;

    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     * @param databaseConnection
     * @return List<Order>
     * @throws SQLException
     */
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("SELECT \"ORDER\".ID AS ORDER_ID, USER.NAME AS USER_NAME, GOODS.NAME AS GOODS_NAME, SUM(\"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM) AS TOTAL_PRICE FROM \"ORDER\" LEFT JOIN USER ON \"ORDER\".USER_ID = USER.ID LEFT JOIN GOODS ON \"ORDER\".GOODS_ID = GOODS.ID GROUP BY \"ORDER\".ID, USER.NAME, GOODS.NAME");
        ArrayList<Order> orders = new ArrayList<>();
        while (resultSet.next()) {
            orders.add(new Order() {{
                id = resultSet.getInt("ORDER_ID");
                userName = resultSet.getString("USER_NAME");
                goodsName = resultSet.getString("GOODS_NAME");
                totalPrice = resultSet.getBigDecimal("TOTAL_PRICE");
            }});
        }
        return orders;
    }

    // 注意，运行这个方法之前，请先运行mvn initialize把测试数据灌入数据库
    public static void main(String[] args) throws SQLException {
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

}
