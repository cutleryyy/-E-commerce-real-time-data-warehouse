import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker('zh_CN')

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# -----------------------------
# 配置（维度维度值，作为事实表的外键）
# -----------------------------
actions = ["view", "click", "add_to_cart"]
weights = [80, 15, 5]

categories = list(range(101, 151))          # 品类ID范围
channels = ["APP", "WEB", "MINI_PROGRAM"]
provinces = ["北京", "上海", "广东", "浙江", "江苏", "四川", "湖北", "福建"]

# -----------------------------
# 共享用户池 & 商品池（保证流之间能关联）
# -----------------------------
registered_users = []          # 已注册用户 ID 池（list 支持 random.choice）
active_products = set()        # 活跃商品 ID 池（有点击/加购行为的商品）

# -----------------------------
# 状态机（用于订单状态流转）
# -----------------------------
state_machine = {
    "CREATED": "PAID",
    "PAID": "SHIPPED",
    "SHIPPED": "REFUNDED"
}

order_cache = {}          # order_id -> 订单信息（缓存用于状态流）
order_status = {}         # order_id -> 当前状态
active_orders = []        # 活跃订单（未终态）

def next_status(order_id):
    current = order_status.get(order_id)
    if current is None or current == "REFUNDED":
        return None
    if current == 'SHIPPED':
        if random.random()<0.3:
            new_status = 'REFUNDED'
            order_status[order_id]=new_status
            return new_status
        else:
            return None
    new_status = state_machine[current]
    order_status[order_id] = new_status
    return new_status

def cleanup(order_id):
    if order_id in active_orders:
        active_orders.remove(order_id)
    order_cache.pop(order_id, None)
    order_status.pop(order_id, None)

# -----------------------------
# 事实数据生成函数（只包含外键和度量值）
# -----------------------------
def generate_click_event():
    product_id = random.randint(1000, 1999)
    # 将出现过的商品加入活跃商品池
    active_products.add(product_id)
    return {
        "user_id": random.randint(1, 100000),
        "action": random.choices(actions, weights=weights)[0],
        "product_id": product_id,
        "category_id": random.choice(categories),
        "province": random.choice(provinces),
        "channel": random.choice(channels),
        "timestamp": int((time.time() - random.randint(0, 20)) * 1000)
    }

def generate_order_event():
    # 决定本次订单用户是否来自已注册用户（80%概率使用已注册用户）
    if registered_users and random.random() < 0.8:
        # 从已注册用户中随机选择一个
        user_id = random.choice(registered_users)
        # 已注册用户视为老用户
        is_new_user = 0
    else:
        # 20%概率产生未注册用户下单（用于分析）
        user_id = random.randint(1, 100000)
        is_new_user = 1

    # productId：优先从活跃商品池中选取
    if active_products and random.random() < 0.7:
        product_id = random.choice(list(active_products))
    else:
        product_id = random.randint(1000, 1999)

    return {
        "order_id": random.randint(100000, 999999),
        "user_id": user_id,
        "product_id": product_id,
        "category_id": random.choice(categories),
        "province": random.choice(provinces),
        "channel": random.choice(channels),
        "is_new_user": is_new_user,
        "amount": random.randint(1000, 50000),   # 单位：分
        "timestamp": int((time.time() - random.randint(0, 20)) * 1000)
    }

def generate_register_event():
    user_id = random.randint(1, 100000)
    registered_users.append(user_id)
    # 使用 user_id 作为随机种子，确保同一 user_id 每次都生成相同属性
    seed = user_id
    rand = random.Random(seed)  # 独立的随机对象

    # 生成确定性字段
    user_name = f"User_{user_id}"  # 或使用 Faker 生成名字，但用 seed 可保持一致性
    age = rand.randint(18, 65)
    gender = rand.choice([0, 1])   # 0-女，1-男
    province = random.choice(provinces)  # 省份可以随机，但为了真实也可以基于 user_id 取模
    vip_level = rand.randint(0, 5)
    register_time = int((time.time() - rand.randint(0, 30)) * 1000)  # 过去一天内

    return {
        "user_id": user_id,
        "user_name": user_name,
        "age": age,
        "gender": gender,
        "province": province,
        "vip_level": vip_level,
        "timestamp": register_time
    }

# -----------------------------
# 主循环
# -----------------------------
print("Kafka 电商模拟数据开始发送...")

try:
    while True:
        # 1. 用户点击流（每轮1-3条，商品池由此构建）
        for _ in range(random.randint(1, 3)):
            click = generate_click_event()
            producer.send(
                "user_click_log",
                key=str(click["user_id"]).encode(),
                value=click
            )

        # 2. 订单流（30% 概率）
        if random.random() < 0.3:
            order = generate_order_event()
            order_id = order["order_id"]

            # 注意：order_paid_log 不在 CREATED 时发送，改为在状态推进到 PAID 时发送

            # 缓存订单信息（用于后续状态流）
            order_cache[order_id] = order
            order_status[order_id] = "CREATED"
            active_orders.append(order_id)

            # 发送 CREATED 状态（事实表字段）
            producer.send(
                "order_status_log",
                key=str(order_id).encode(),
                value={
                    "order_id": order_id,
                    "status": "CREATED",
                    "user_id": order["user_id"],
                    "product_id": order["product_id"],
                    "category_id": order["category_id"],
                    "province": order["province"],
                    "channel": order["channel"],
                    "is_new_user": order["is_new_user"],
                    "amount": order["amount"],
                    "timestamp": int(time.time() * 1000)
                }
            )

        # 3. 注册流（15% 概率，加速用户池积累）
        if random.random() < 0.15:
            register = generate_register_event()
            producer.send(
                "user_register_log",
                key=str(register["user_id"]).encode(),
                value=register
            )

        # 4. 状态推进（按顺序推进已有订单状态）
        for order_id in list(active_orders):
            new_status = next_status(order_id)
            if new_status is None:
                cleanup(order_id)
                continue

            info = order_cache.get(order_id)
            if not info:
                continue

            producer.send(
                "order_status_log",
                key=str(order_id).encode(),
                value={
                    "order_id": order_id,
                    "status": new_status,
                    "user_id": info["user_id"],
                    "product_id": info["product_id"],
                    "category_id": info["category_id"],
                    "province": info["province"],
                    "channel": info["channel"],
                    "is_new_user": info["is_new_user"],
                    "amount": info["amount"],
                    "timestamp": int(time.time() * 1000)
                }
            )

            # === 关键修复：只有在 PAID 状态时才发送 order_paid_log ===
            if new_status == "PAID":
                # 字段名用 eventTime 以匹配 OrderPaidLog 的 Java Bean 命名
                paid_event = {
                    "order_id": order_id,
                    "user_id": info["user_id"],
                    "product_id": info["product_id"],
                    "category_id": info["category_id"],
                    "amount": info["amount"],
                    "eventTime": int(time.time() * 1000)
                }
                producer.send(
                    "order_paid_log",
                    key=str(order_id).encode(),
                    value=paid_event
                )

        producer.flush()
        time.sleep(1)

except Exception as e:
    print("异常:", e)
    time.sleep(1)

finally:
    producer.flush()
    producer.close()
    print("生成器已停止")
