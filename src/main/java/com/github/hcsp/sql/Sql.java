
package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Sql {
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
    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        Integer result;
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select count(distinct user_id) as count from `order` where goods_id = ?")) {
            preparedStatement.setInt(1, goodsId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                result = null;
                if (resultSet.next()) {
                    result = Integer.parseInt(resultSet.getString("count"));
                }
            }
        }
        return result;
    }

    /**
     * 题目2：
     * 分页查询所有用户，按照ID倒序排列
     *
     * @param pageNum  第几页，从1开始
     * @param pageSize 每页有多少个元素
     * @return 指定页中的用户
     */
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        ArrayList<User> users;
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select * from user order by id desc limit ?,?")) {
            preparedStatement.setInt(1, (pageNum - 1) * pageSize);
            preparedStatement.setInt(2, pageSize);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                users = new ArrayList<>();
                while (resultSet.next()) {
                    User user = new User();
                    user.id = resultSet.getInt("id");
                    user.name = resultSet.getString("name");
                    user.tel = resultSet.getString("tel");
                    user.address = resultSet.getString("address");
                    users.add(user);
                }
            }
        }
        return users;
    }

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
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        ArrayList<GoodsAndGmv> goodsAndGmvs;
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select g.id, g.name,O.GOODS_PRICE * sum(O.GOODS_NUM) as GVM\n" +
                "from GOODS g join \"ORDER\" O on g.ID = O.GOODS_ID group by g.NAME order by GVM desc;")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                goodsAndGmvs = new ArrayList<>();
                while (resultSet.next()) {
                    GoodsAndGmv goodsAndGmv = new GoodsAndGmv();
                    goodsAndGmv.goodsId = resultSet.getInt("id");
                    goodsAndGmv.goodsName = resultSet.getString("name");
                    goodsAndGmv.gmv = resultSet.getBigDecimal("gvm");
                    goodsAndGmvs.add(goodsAndGmv);
                }
            }
        }
        return goodsAndGmvs;
    }

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
    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        ArrayList<Order> orders;
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select O.ID, U.name as username, G.NAME as goodsName, O.GOODS_PRICE * O.GOODS_NUM as TOTAL_PRICE\n" +
                "from \"ORDER\" O\n" +
                "         join GOODS G on G.ID = O.GOODS_ID\n" +
                "         join USER U on U.ID = O.USER_ID;")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                orders = new ArrayList<>();
                while (resultSet.next()) {
                    Order order = new Order();
                    order.id = resultSet.getInt("id");
                    order.userName = resultSet.getString("username");
                    order.goodsName = resultSet.getString("goodsname");
                    order.totalPrice = resultSet.getBigDecimal("total_price");
                    orders.add(order);

                }
            }
        }
        return orders;
    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     */
    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        ArrayList<Order> orders;
        try (PreparedStatement preparedStatement = databaseConnection.prepareStatement("select O.ID as order_id, U.NAME as user_name, G.NAME as goods_name, O.GOODS_NUM * O.GOODS_PRICE as total_price\n" +
                "from \"ORDER\" O\n" +
                "         left join GOODS G on G.ID = O.GOODS_ID\n" +
                "         left join USER U on U.ID = O.USER_ID;")) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                orders = new ArrayList<>();
                while (resultSet.next()) {
                    Order order = new Order();
                    order.id = resultSet.getInt("order_id");
                    order.userName = resultSet.getString("user_name");
                    order.goodsName = resultSet.getString("goods_name");
                    order.totalPrice = resultSet.getBigDecimal("total_price");
                    orders.add(order);
                }
            }
        }
        return orders;
    }

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
