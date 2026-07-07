import pymysql
import redis

# mysql连接配置
mysql_conn = pymysql.connect(
    host="192.168.94.200",
    port=3306,
    user='root',
    password='root123',
    database='realtime_db',
    charset='utf8mb4'
)
print("MySQL连接成功！")
# redis连接配置
redis_client = redis.Redis(host='192.168.94.200', port=6379, db=0, decode_responses=True)

# 定义MySQL查询
cursor = mysql_conn.cursor()
cursor.execute('select product_id,product_name,category_id,category_name,price from dim_product')

rows = cursor.fetchall()
for row in rows:
    product_id, product_name, category_id, category_name, price = row
    key = f"dim:product:{product_id}"
    #     使用hash存储
    redis_client.hset(key, mapping={
        'product_id': product_id,
        'product_name': product_name if product_name is not None else '',
        'category_id': category_id if category_id is not None else 0000,
        'category_name': category_name if category_name is not None else '其他',
        'price': float(price) if price is not None else 0.0
    })
cursor.close()
mysql_conn.close()
print("product维度表创建成功！")
