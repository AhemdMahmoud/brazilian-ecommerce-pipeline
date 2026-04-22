

-- please run all the next statment on terminal 


--------------------------------------------------------------------creatae folder on hdfs ---------------------------------------------------------------------------
hdfs dfs -mkdir -p /user/root/ecommerce/customers 
hdfs dfs -mkdir -p /user/root/ecommerce/orders 

hdfs dfs -mkdir -p /user/root/ecommerce/order_items  
hdfs dfs -mkdir -p /user/root/ecommerce/reviews  

hdfs dfs -mkdir -p /user/root/ecommerce/products  


// guys if you want to run this iinstead from the prevouse it's professional  
--run on termainl or put it on bash script , i'm run on terminal ? 



for t in customers orders order_items reviews products
do
  hdfs dfs -mkdir -p /user/root/ecommerce/$t
done

---------------------------------- 
----------------------------------------------------------------------------sqoop_cmd_mysql_hdfs------------------------------------
sqoop import \
--connect jdbc:mysql://localhost:3306/ecommerce \
--username root \
--password your_password \
--table customers \
--target-dir /user/root/ecommerce/customers \
--m 1



sqoop import \
--connect jdbc:mysql://localhost:3306/ecommerce \
--username root \
--password your_password \
--table orders \
--target-dir /user/root/ecommerce/orders \
--m 1








sqoop import \
--connect jdbc:mysql://localhost:3306/ecommerce \
--username root \
--password your_password \
--table order_items \
--target-dir /user/root/ecommerce/order_items \
--m 1 \
--split-by order_item_id



sqoop import \
--connect jdbc:mysql://localhost:3306/ecommerce \
--username root \
--password your_password \
--table reviews \
--target-dir /user/root/ecommerce/reviews \
--m 1




sqoop import \
--connect jdbc:mysql://localhost:3306/ecommerce \
--username root \
--password your_password \
--table products \
--target-dir /user/root/ecommerce/products \
--m 1














