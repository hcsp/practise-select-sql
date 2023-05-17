
package com.github.hcsp.sql;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Sql {
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

    public static int countUsersWhoHaveBoughtGoods(Connection databaseConnection, Integer goodsId) throws SQLException {
        PreparedStatement ps = databaseConnection.prepareStatement("select count(distinct USER_ID)\n" +
                "from \"ORDER\"\n" +
                "where GOODS_ID = ?;");
        ps.setInt(1, goodsId);
        ResultSet resultSet = ps.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
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
    public static List<User> getUsersByPageOrderedByIdDesc(Connection databaseConnection, int pageNum, int pageSize) throws SQLException {
        List<User> users = new ArrayList<>();
        PreparedStatement ps = databaseConnection.prepareStatement("select  id, name, tel, address from USER order by ID desc limit ?, ?;");
        int searchStartIndex = (pageNum - 1) * pageSize;
        int searchEndIndex = searchStartIndex + pageNum;
        ps.setInt(1, searchStartIndex);
        ps.setInt(2, searchEndIndex);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            User user = new User();
            user.id = resultSet.getInt("ID");
            user.name = resultSet.getString("NAME");
            user.tel = resultSet.getString("TEL");
            user.address = resultSet.getString("ADDRESS");
            users.add(user);
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
     */
    public static List<GoodsAndGmv> getGoodsAndGmv(Connection databaseConnection) throws SQLException {
        List<GoodsAndGmv> goodsAndGmvs = new ArrayList<>();
        PreparedStatement ps = databaseConnection.prepareStatement("select  G2.ID,G2.NAME, sum(GOODS_PRICE * GOODS_NUM) as GMV\n"
                + "from \"ORDER\"\n"
                + "join GOODS G2 on G2.ID = \"ORDER\".GOODS_ID\n"
                + "group by GOODS_ID\n"
                + "order by GMV desc ;");
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            GoodsAndGmv goodsAndGmv = new GoodsAndGmv();
            goodsAndGmv.gmv = resultSet.getBigDecimal("GMV");
            goodsAndGmv.goodsId = resultSet.getInt("ID");
            goodsAndGmv.goodsName = resultSet.getString("NAME");
            goodsAndGmvs.add(goodsAndGmv);
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
     */

    public static List<Order> getInnerJoinOrders(Connection databaseConnection) throws SQLException {
        List<Order> orders = new ArrayList<>();
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("select \"ORDER\".ID                      as ORDER_ID,\n" +
                "       U.NAME                          as USER_NAME,\n" +
                "       G2.NAME                         as GOODS_NAME,\n" +
                "       \"ORDER\".GOODS_NUM * GOODS_PRICE as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "         join USER U on U.ID = \"ORDER\".USER_ID\n" +
                "         join GOODS G2 on G2.ID = \"ORDER\".GOODS_ID");

        return getOrders((List<Order>) orders, resultSet);
    }

    private static List<Order> getOrders(List<Order> orders, ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            Order order = new Order();
            order.goodsName = resultSet.getString("GOODS_NAME");
            order.userName = resultSet.getString("USER_NAME");
            order.id = resultSet.getInt("ORDER_ID");
            order.totalPrice = resultSet.getBigDecimal("TOTAL_PRICE");
            orders.add(order);
        }
        return orders;
    }

    /**
     * 题目5：
     * 查询所有订单信息，哪怕它的用户名、商品名缺失，即LEFT JOIN方式
     */

    public static List<Order> getLeftJoinOrders(Connection databaseConnection) throws SQLException {
        List<Order> orders = new ArrayList<>();
        ResultSet resultSet = databaseConnection.createStatement().executeQuery("select \"ORDER\".ID                      as ORDER_ID,\n" +
                "       U.NAME                          as USER_NAME,\n" +
                "       G2.NAME                         as GOODS_NAME,\n" +
                "       \"ORDER\".GOODS_NUM * GOODS_PRICE as TOTAL_PRICE\n" +
                "from \"ORDER\"\n" +
                "        left join USER U on U.ID = \"ORDER\".USER_ID\n" +
                "        left join GOODS G2 on G2.ID = \"ORDER\".GOODS_ID");

        return getOrders((List<Order>) orders, resultSet);
    }


}
