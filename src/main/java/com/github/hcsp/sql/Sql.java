
package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * -- select * from GOODS;
 * -- show tables;
 * -- SELECT COUNT(*) FROM USER WHERE ADDRESS='shanghai';
 * -- SELECT ID,NAME FROM USER WHERE ADDRESS='shanghai' order by ID desc;
 * -- pagination
 * -- select * from USER limit 0,2; -- limit(from, maxnumber)
 * -- groupby
 * -- select ADDRESS from USER group by ADDRESS; -- select 的要和 groupby 的一一对应
 * -- select ADDRESS, count(*) from USER group by ADDRESS;
 * -- select GOODS_ID, count(*) as count from `ORDER` group by GOODS_ID; -- name alias
 * -- select GOODS_ID, SUM(GOODS_NUM*GOODS_PRICE) as total from "ORDER" group by GOODS_ID order by total desc;
 * <p>
 * --join
 * -- select "ORDER".id, "ORDER".GOODS_ID, "ORDER".USER_ID, GOODS.NAME
 * -- from "ORDER"
 * --     join GOODS on GOODS.ID = "ORDER".GOODS_ID; -- inner join
 * <p>
 * -- select "ORDER".id, "ORDER".GOODS_ID, "ORDER".USER_ID, GOODS.NAME, U.NAME
 * -- from "ORDER"
 * --          left join GOODS on GOODS.ID = "ORDER".GOODS_ID; -- left join
 * select "ORDER".id, "ORDER".GOODS_ID, "ORDER".USER_ID, GOODS.NAME, U.NAME, U.ADDRESS
 * from "ORDER"
 * join GOODS on GOODS.ID = "ORDER".GOODS_ID
 * join USER U on U.ID = "ORDER".USER_ID where U.ADDRESS = 'shenzhen'; -- left join
 * <p>
 * --查询有多少所有用户曾经买过指定的商品
 * -- select count(DISTINCT USER_ID) from "ORDER" where GOODS_ID = 1;
 * <p>
 * select * from user where id in (select USER_ID from "ORDER" where GOODS_ID = '1')
 */
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
    // 例如，输入goodsId = 1，返回2，因为有2个用户曾经买过商品1。
// +-----+
// |count|
// +-----+
// | 2   |
// +-----+
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        // select count(DISTINCT USER_ID) from "ORDER" where GOODS_ID = 1;
        // select * from user where id in (select USER_ID from "ORDER" where GOODS_ID = '1')
        try (PreparedStatement statement = databaseConnection.prepareStatement("select count(Distinct USER_ID) from `ORDER` where GOODS_ID = ?")) {
            statement.setInt(1, goodsId);
            ResultSet result = statement.executeQuery();
            if (result.next()) {
                return result.getInt(1);
            }
        }
        return 0;
    }

    // 例如，pageNum = 2, pageSize = 3（每页3个元素，取第二页），则应该返回：
// +----+----------+------+----------+
// | ID | NAME     | TEL  | ADDRESS  |
// +----+----------+------+----------+
// | 1  | zhangsan | tel1 | beijing  |
// +----+----------+------+----------+
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select ID, NAME, TEL, ADDRESS from USER order by ID desc limit ?, ?")) {
            preparedStatement.setInt(1, (pageNum - 1) * pageSize);
            preparedStatement.setInt(2, pageSize);
            ResultSet resultSet = preparedStatement.executeQuery();
            List<User> users = new ArrayList<>();
            while (resultSet.next()) {
                User user = new User();
                user.id = resultSet.getInt(1);
                user.name = resultSet.getString(2);
                user.tel = resultSet.getString(3);
                user.address = resultSet.getString(4);
                users.add(user);
            }
            return users;
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
    // 查询所有的商品及其销售额，按照销售额从大到小排序
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
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        //select  GOODS_ID, G2.NAME, SUM(GOODS_NUM * GOODS_PRICE) as ORDER_SALE from "ORDER" JOIN GOODS G2 on G2.ID = "ORDER".GOODS_ID group by GOODS_ID;
        try (PreparedStatement statement = databaseConnection.prepareStatement("select  GOODS_ID, G2.NAME, SUM(GOODS_NUM * GOODS_PRICE) as ORDER_SALE from \"ORDER\" JOIN GOODS G2 on G2.ID = \"ORDER\".GOODS_ID group by GOODS_ID order by ORDER_SALE desc")) {
            ResultSet resultSet = statement.executeQuery();
            List<GoodsAndGmv> list = new ArrayList<>();
            while (resultSet.next()) {
                GoodsAndGmv gmv = new GoodsAndGmv();
                gmv.goodsId = resultSet.getInt(1);
                gmv.goodsName = resultSet.getString(2);
                gmv.gmv = resultSet.getBigDecimal(3);
                list.add(gmv);
            }
            return list;
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
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        try (PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".id as ORDER_ID, U.NAME as USER_NAME, G2.NAME as GOODS_NAME, \"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM as TOTAL_PRICE\n" + "from \"ORDER\" join GOODS G2 on G2.ID = \"ORDER\".GOODS_ID join USER U on U.ID = \"ORDER\".USER_ID")) {
            return getOrders(statement);
        }
    }

    private static List<Order> getOrders(PreparedStatement statement) throws SQLException {
        ResultSet resultSet = statement.executeQuery();
        List<Order> result = new ArrayList<>();
        while (resultSet.next()) {
            Order newOrder = new Order();
            newOrder.id = resultSet.getInt(1);
            newOrder.userName = resultSet.getString(2); // 用户名;
            newOrder.goodsName = resultSet.getString(3);
            newOrder.totalPrice = resultSet.getBigDecimal(4); // 订单总金额
            result.add(newOrder);
        }
        return result;
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
// | 7        | NULL      | goods4     | 20          |
// +----------+-----------+------------+-------------+
// | 8        | NULL      | NULL       | 60          |
// +----------+-----------+------------+-------------+
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        //select "ORDER".id as ORDER_ID, U.NAME as USER_NAME, G2.NAME as GOODS_NAME, "ORDER".GOODS_PRICE * "ORDER".GOODS_NUM as TOTAL_PRICE
        //from "ORDER" join GOODS G2 on G2.ID = "ORDER".GOODS_ID left join USER U on U.ID = "ORDER".USER_ID;
        try (PreparedStatement statement = databaseConnection.prepareStatement("select \"ORDER\".id as ORDER_ID, U.NAME as USER_NAME, " + "G2.NAME as GOODS_NAME, \"ORDER\".GOODS_PRICE * \"ORDER\".GOODS_NUM as TOTAL_PRICE\n" + "from \"ORDER\"" + "left join GOODS G2 on G2.ID = \"ORDER\".GOODS_ID " + "left join USER U on U.ID = \"ORDER\".USER_ID;")) {
            return getOrders(statement);
        }
    }

    // 注意，运行这个方法之前，请先运行mvn initialize把测试数据灌入数据库Ò
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
