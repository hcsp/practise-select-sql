package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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


// 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+

    /**
     * 题目1：
     * 查询有多少用户曾经买过指定的商品
     *
     * @param databaseConnection c
     * @param goodsId            指定的商品ID
     * @return 有多少用户买过这个商品
     * @throws SQLException s
     */
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        String sql = "select count(distinct USER_ID) from `order` where GOODS_ID = ?";

        try (PreparedStatement statement = databaseConnection.prepareStatement(sql)) {
            statement.setInt(1, goodsId);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }


// 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param databaseConnection c
     * @param pageNum            第几页，从1开始
     * @param pageSize           每页有多少个元素
     * @return 指定页中的用户
     * @throws SQLException s
     */
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws
            SQLException {
        String sql = "select * from USER u order by u.ID desc limit ?,?;";


        try (PreparedStatement statement = databaseConnection.prepareStatement(sql)) {
            statement.setInt(1, (pageNum - 1) * pageSize);
            statement.setInt(2, pageSize);
            try (ResultSet rs = statement.executeQuery()) {

                return StreamSupport.stream(new Spliterators.AbstractSpliterator<User>(Long.MAX_VALUE, Spliterator.ORDERED) {
                    @Override
                    public boolean tryAdvance(Consumer<? super User> action) {
                        try {
                            if (!rs.next()) {
                                return false;
                            }
                            User line = new User();
                            line.id = rs.getInt("ID");
                            line.name = rs.getString("NAME");
                            line.tel = rs.getString("TEL");
                            line.address = rs.getString("ADDRESS");
                            action.accept(line);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        return true;
                    }
                }, false).collect(Collectors.toList());
            }
        }
    }


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

    /**
     * 题目3：
     * 查询所有的商品及其销售额，按照销售额从大到小排序
     *
     * @param databaseConnection c
     * @return 查询所有的商品及其销售额，按照销售额从大到小排序
     * @throws SQLException s
     */
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        String sql = "select g.ID, sum(GOODS_NUM * GOODS_PRICE) as GMV, g.NAME as NAME\n" +
                "from GOODS g\n" +
                "         left join \"ORDER\" o\n" +
                "where g.ID = o.GOODS_ID\n" +
                "group by g.ID\n" +
                "order by GMV desc";

        PreparedStatement ps = databaseConnection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        List<GoodsAndGmv> resList = new ArrayList<>();
        while (rs.next()) {
            GoodsAndGmv line = new GoodsAndGmv();
            line.goodsId = rs.getInt("ID");
            line.goodsName = rs.getString("NAME");
            line.gmv = rs.getBigDecimal("GMV");
            resList.add(line);
        }
        return resList;
    }


// 预期的结果为：
// +----------+-----------+------------+-------------+
// | ORDER_ID | USER_NAME | GOODS_NAME | TOTAL_PRICE |
// +----------+-----------+------------+-------------+
// | 1        | zhangsan  | goods1     | 50          |
// +----------+-----------+------------+-------------+
// | 2        | lisi      | goods1     | 10          |
// +----------+-----------+------------+-------------+
// | 3        | lisi      | goods1     | 20          |
// +----------+-----------+------------+-------------+
// | 4        | zhangsan  | goods2     | 80          |
// +----------+-----------+------------+-------------+
// | 5        | zhangsan  | goods2     | 2000        |
// +----------+-----------+------------+-------------+
// | 6        | zhangsan  | goods3     | 20          |
// +----------+-----------+------------+-------------+

    /**
     * 题目4：
     * 查询订单信息，只查询用户名、商品名齐全的订单，即INNER JOIN方式
     *
     * @param databaseConnection c
     * @return 查询所有的商品及其销售额，按照销售额从大到小排序
     * @throws SQLException s
     */
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        String sql = "select o.ID as ORDER_ID, u.NAME as GOODS_NAME, g.NAME as USER_NAME, o.GOODS_NUM as TOTAL_PRICE\n" +
                "from \"ORDER\" o\n" +
                "         inner join GOODS g on o.GOODS_ID = g.ID\n" +
                "         inner join USER u on o.USER_ID = u.ID\n" +
                "where g.NAME is not null\n" +
                "  and u.NAME is not null";
        PreparedStatement ps = databaseConnection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        List<Order> resList = new ArrayList<>();
        while (rs.next()) {
            Order line = new Order();
            line.id = rs.getInt("ORDER_ID");
            line.goodsName = rs.getString("GOODS_NAME");
            line.userName = rs.getString("USER_NAME");
            line.totalPrice = rs.getBigDecimal("TOTAL_PRICE");
            resList.add(line);
        }
        return resList;
    }


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

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     *
     * @param databaseConnection c
     * @return 查询所有的商品及其销售额，按照销售额从大到小排序
     * @throws SQLException s
     */
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        PreparedStatement ps = databaseConnection.prepareStatement("SELECT O.ID ORDER_ID, U.NAME USER_NAME, G.NAME GOODS_NAME, O.GOODS_PRICE * O.GOODS_NUM TOTAL_PRICE FROM `ORDER` O LEFT JOIN GOODS G ON O.GOODS_ID = G.ID LEFT JOIN USER U ON O.USER_ID = U.ID");
        ResultSet rs = ps.executeQuery();
        List<Order> resList = new ArrayList<>();
        while (rs.next()) {
            Order line = new Order();
            line.id = rs.getInt("ORDER_ID");
            line.goodsName = rs.getString("GOODS_NAME");
            line.userName = rs.getString("USER_NAME");
            line.totalPrice = rs.getBigDecimal("TOTAL_PRICE");
            resList.add(line);
        }
        return resList;
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

    @FunctionalInterface
    interface AddFromResult {
        User actionPerformed(ResultSet resultSet);
    }

    // 用户信息
    public static class User implements AddFromResult {
        Integer id;
        String name;
        String tel;
        String address;

        @Override
        public String toString() {
            return "User{" + "id=" + id + ", name='" + name + '\'' + ", tel='" + tel + '\'' + ", address='" + address + '\'' + '}';
        }

        @Override
        public User actionPerformed(ResultSet resultSet) {
            return null;
        }
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

}
