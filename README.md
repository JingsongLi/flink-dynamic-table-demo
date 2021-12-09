# Flink Dynamic Table Demo

## Demo 组件
- Mysql
- Datagen 持续生成数据到 Mysql
- Kafka
- Flink
- Zeppelin

使用 Docker 启动上述组件，请保证 Docker 内存大于 4G (推荐 6G)。([参考链接](https://docs.docker.com/desktop/mac/))

## 准备工作
- `docker-compose up -d`
- 打开 localhost:8080 ，进入 Zeppelin 界面
- 点击右上角
- 点击 Interpreter
- 搜索 flink
- 配置 FLINK_HOME 为 `/opt/flink-1.15-SNAPSHOT`
- 拉到下面，选 SAVE
- 点击左上 Notebook，create new note
- 自定义名字，选择 flink，点击 create
- 执行 show tables; 查看 Flink UI: localhost:8081

## 流式数仓
![image](https://user-images.githubusercontent.com/9601882/145389495-0f0dad27-9e6d-457e-971d-9a4844151e2b.png)

Mysql cdc DDLs:
```
-- Mysql CDC：订单表
CREATE TEMPORARY TABLE orders (
    order_id VARCHAR,
    cate_id VARCHAR,
    trans_amount BIGINT,
    gmt_create VARCHAR,
    dt AS DATE_FORMAT(gmt_create, 'yyyy-MM-dd'),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
	'connector' = 'mysql-cdc',
	 'hostname' = 'mysql',
	 'port' = '3306',
	 'username' = 'root',
	 'password' = '123456',
	 'database-name' = 'retail',
	 'table-name' = 'orders'
);

--Mysql CDC：类目表
CREATE TEMPORARY TABLE cate_dim (
    cate_id VARCHAR,
    parent_cate_id VARCHAR,
    PRIMARY KEY (cate_id) NOT ENFORCED
) WITH (
	'connector' = 'mysql-cdc',
	 'hostname' = 'mysql',
	 'port' = '3306',
	 'username' = 'root',
	 'password' = '123456',
	 'database-name' = 'retail',
	 'table-name' = 'category'
);
```

Dynamic Table DDLs:
```
-- Flink 动态表：DWD 订单类目宽表
CREATE TEMPORARY TABLE dwd_orders_cate (
    dt STRING,
    parent_cate_id VARCHAR,
    cate_id VARCHAR,
    order_id VARCHAR,
    trans_amount BIGINT,
    gmt_create STRING,
PRIMARY KEY (order_id, dt) NOT ENFORCED
) PARTITIONED BY (dt);

-- Flink 动态表：DWS 类目指标聚合表
CREATE  TABLE dws_cate_day (
    dt STRING,
    parent_cate_id VARCHAR,
    cate_gmv BIGINT,
    PRIMARY KEY (parent_cate_id, dt) NOT ENFORCED
) PARTITIONED BY (dt);
```

Streaming pipeline:
```
-- 流作业：两张Mysql cdc表join写入DWD
INSERT INTO dwd_orders_cate
SELECT
    s.dt,
    d.parent_cate_id,
    s.cate_id,
    s.order_id,
    s.trans_amount,
    s.gmt_create 
FROM `orders` s  INNER JOIN `cate_dim` `d`
ON s.cate_id = d.cate_id;

-- 流作业：DWD经过聚合写入DWS
INSERT INTO dws_cate_day
SELECT
    dt,
    parent_cate_id,
    SUM(trans_amount) AS cate_gmv
FROM dwd_orders_cate
GROUP BY parent_cate_id, dt;
```

## OLAP 查询

请修改对应的日期：
```
-- 实时OLAP：Join 订单宽表和类目指标表，得出订单在这个类目下金额的占比
SELECT
  order_id,
  trans_amount,
  CAST(trans_amount AS DOUBLE) / cate_gmv AS ratio
  FROM dwd_orders_cate d JOIN dws_cate_day s
  ON d.parent_cate_id = s.parent_cate_id -- Join condition
  WHERE d.dt = '${TODAY}' AND s.dt = '${TODAY}' -- 分区Pruning
  ORDER BY ratio DESC LIMIT 10;
  
-- 历史OLAP：查询看订单宽表三天前的数据
SELECT * FROM dwd_orders_cate WHERE dt = '${3-days-ago}';
```

## 数据订正
![image](https://user-images.githubusercontent.com/9601882/145390269-35318825-6d8c-4e00-9396-37b30178bc0e.png)

请修改对应的日期：
```
-- Batch统计：查看有脏数据的分区
SELECT DISTINCT dt FROM dwd_orders_cate WHERE trans_amount <= 0;

--Batch 数据订正：覆写指定分区
INSERT OVERWRITE dws_cate_day PARTITION (dt = '${3-days-ago}')
SELECT
    parent_cate_id,
    SUM(trans_amount) AS cate_gmv
FROM dwd_orders_cate 
WHERE dt = '${3-days-ago}' AND trans_amount > 0
GROUP BY parent_cate_id;

--OLAP查询：查看订正后数据
SELECT * FROM dws_cate_day WHERE dt = '${3-days-ago}';
```

## 谢谢尝试
