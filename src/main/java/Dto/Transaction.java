package Dto;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    private String transaction_id;
    private String product_id;
    private String product_name;
    private String product_category;
    private double product_price;
    private int product_quantity;
    private String product_brand;
    private double total_amount;
    private String currency;
    private String customer_id;
    private Timestamp transaction_date;
    private String payment_method;
}
