-- 电商交易实时数仓：离线 T+1 的 Hive 库表初始化
-- 对应 docs/03-数据模型.md 十二、离线 T+1（Hive）分层；docs/04-架构设计.md 4.2 作业清单；docs/06-数据质量与对账.md 六
-- 是什么：Hive 侧 ods / dwd / dws / ads 四层表结构，Parquet 存储，事实表按 dt 分区
-- 干什么：spark/offline_t1.py 每次覆盖写一个 dt 分区；spark/reconcile.py 读 ads.trade_recon_1d 与 Doris 比对
-- 执行：/home/hzh/apps/spark/bin/spark-sql -f sql/09_init_hive.sql
-- 说明：
--   1. 分区列统一用 STRING 'yyyy-MM-dd'：DATE 分区在 Spark / Hive / metastore 之间转换容易出错，
--      查询时用 dt = '2026-11-25' 即可，需要日期运算时 CAST(dt AS DATE)。
--   2. 维表（shop / user / product）不带分区，每次跑批全量覆盖——它们是"当前值"，没有按日回溯的需求。
--   3. 支付 / 退款在抽取时就 JOIN 订单落成 DWD 明细：ODS 按事件日分区，单天分区里拿不到跨天订单的
--      user_id / is_deleted / 下单时间，而支付用户数、退款按原订单日归因都要这些字段。

CREATE DATABASE IF NOT EXISTS ods
  COMMENT '离线 ODS：MySQL 业务库按天快照' LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db';
CREATE DATABASE IF NOT EXISTS dwd
  COMMENT '离线 DWD：明细宽表' LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dwd.db';
CREATE DATABASE IF NOT EXISTS dws
  COMMENT '离线 DWS：天级聚合' LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dws.db';
CREATE DATABASE IF NOT EXISTS ads
  COMMENT '离线 ADS：对账口径结果' LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ads.db';

-- ---------------------------------------------------------------- ODS
CREATE EXTERNAL TABLE IF NOT EXISTS ods.order_info (
  order_id     BIGINT        COMMENT '订单ID',
  user_id      BIGINT        COMMENT '用户ID',
  shop_id      BIGINT        COMMENT '店铺ID',
  order_amount DECIMAL(18,2) COMMENT '下单金额',
  item_cnt     INT           COMMENT '商品件数',
  order_status SMALLINT      COMMENT '100待支付 200已支付 300已发货 400已签收 500已完成 600已取消',
  is_deleted   TINYINT       COMMENT '逻辑删除',
  create_time  TIMESTAMP     COMMENT '下单时间',
  update_time  TIMESTAMP     COMMENT '源端更新时间'
) COMMENT '订单主表按天下单快照'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db/order_info';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.order_detail (
  detail_id   BIGINT        COMMENT '明细ID',
  order_id    BIGINT        COMMENT '订单ID',
  product_id  BIGINT        COMMENT '商品ID',
  num         INT           COMMENT '数量',
  price       DECIMAL(18,2) COMMENT '成交单价',
  amount      DECIMAL(18,2) COMMENT '小计',
  create_time TIMESTAMP     COMMENT '创建时间'
) COMMENT '订单明细按天快照'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db/order_detail';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.shop_info (
  shop_id     BIGINT      COMMENT '店铺ID',
  shop_name   STRING      COMMENT '店铺名',
  shop_type   STRING      COMMENT '店铺类型',
  province    STRING      COMMENT '省份',
  city        STRING      COMMENT '城市',
  create_time TIMESTAMP   COMMENT '创建时间',
  update_time TIMESTAMP   COMMENT '更新时间'
) COMMENT '店铺维表全量快照'
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db/shop_info';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.user_info (
  user_id       BIGINT    COMMENT '用户ID',
  gender        TINYINT   COMMENT '1男 2女 0未知',
  age           TINYINT   COMMENT '年龄',
  age_range     STRING    COMMENT '年龄段',
  city          STRING    COMMENT '城市',
  user_level    TINYINT   COMMENT '会员等级',
  register_time TIMESTAMP COMMENT '注册时间',
  update_time   TIMESTAMP COMMENT '更新时间'
) COMMENT '用户维表全量快照'
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db/user_info';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.product_info (
  product_id    BIGINT        COMMENT '商品ID',
  product_name  STRING        COMMENT '商品名',
  category_id   BIGINT        COMMENT '一级类目ID',
  category_name STRING        COMMENT '一级类目名',
  brand         STRING        COMMENT '品牌',
  price         DECIMAL(18,2) COMMENT '当前售价',
  status        TINYINT       COMMENT '1上架 0下架',
  update_time   TIMESTAMP     COMMENT '更新时间'
) COMMENT '商品维表全量快照'
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ods.db/product_info';

-- ---------------------------------------------------------------- DWD
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_payment_detail (
  pay_id            BIGINT        COMMENT '支付流水号',
  order_id          BIGINT        COMMENT '订单ID',
  user_id           BIGINT        COMMENT '下单用户ID，来自订单',
  shop_id           BIGINT        COMMENT '店铺ID，来自订单',
  pay_amount        DECIMAL(18,2) COMMENT '支付金额',
  pay_channel       STRING        COMMENT '支付渠道',
  pay_status        TINYINT       COMMENT '1成功 2失败 3退款中',
  pay_time          TIMESTAMP     COMMENT '支付时间',
  order_create_time TIMESTAMP     COMMENT '下单时间，来自订单',
  is_deleted        TINYINT       COMMENT '订单逻辑删除，来自订单'
) COMMENT '支付明细宽表，dt 为支付日期；抽取时关联订单补 user/shop/下单时间'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dwd.db/dwd_payment_detail';

CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_refund_detail (
  refund_id         BIGINT        COMMENT '退款单号',
  order_id          BIGINT        COMMENT '订单ID',
  user_id           BIGINT        COMMENT '下单用户ID，来自订单',
  shop_id           BIGINT        COMMENT '店铺ID，来自订单',
  refund_amount     DECIMAL(18,2) COMMENT '退款金额',
  refund_status     TINYINT       COMMENT '0申请 1成功 2处理中 3拒绝',
  refund_reason     STRING        COMMENT '退款原因',
  refund_time       TIMESTAMP     COMMENT '退款时间',
  order_create_time TIMESTAMP     COMMENT '下单时间，来自订单',
  is_deleted        TINYINT       COMMENT '订单逻辑删除，来自订单'
) COMMENT '退款明细宽表，dt 为退款日期；按原订单日对账时用 order_create_time'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dwd.db/dwd_refund_detail';

CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_trade_order_detail (
  order_id      BIGINT        COMMENT '订单ID',
  user_id       BIGINT        COMMENT '用户ID',
  shop_id       BIGINT        COMMENT '店铺ID',
  province      STRING        COMMENT '店铺所在省',
  city          STRING        COMMENT '店铺所在市',
  order_amount  DECIMAL(18,2) COMMENT '下单金额',
  item_cnt      INT           COMMENT '商品件数',
  pay_id        BIGINT        COMMENT '最近一次成功支付流水号',
  pay_amount    DECIMAL(18,2) COMMENT '最近一次成功支付金额',
  refund_amount DECIMAL(18,2) COMMENT '最近一次成功退款金额',
  create_time   TIMESTAMP     COMMENT '下单时间',
  pay_time      TIMESTAMP     COMMENT '支付时间，未支付为 NULL',
  order_status  SMALLINT      COMMENT '订单状态',
  is_deleted    TINYINT       COMMENT '逻辑删除',
  update_time   TIMESTAMP     COMMENT '源端更新时间'
) COMMENT '离线交易明细，dt 为下单日期；支付/退款取截至跑批时刻的最新一条成功记录'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dwd.db/dwd_trade_order_detail';

-- ---------------------------------------------------------------- DWS
CREATE EXTERNAL TABLE IF NOT EXISTS dws.trade_1d (
  order_cnt     BIGINT        COMMENT '下单笔数，按下单日',
  order_amt     DECIMAL(18,2) COMMENT '下单金额，按下单日',
  pay_cnt       BIGINT        COMMENT '支付笔数，按支付日',
  pay_amt       DECIMAL(18,2) COMMENT '支付金额（支付 GMV），按支付日',
  pay_user_cnt  BIGINT        COMMENT '支付用户数，按支付日，排除软删订单',
  refund_cnt    BIGINT        COMMENT '退款笔数，按退款发生日',
  refund_amt    DECIMAL(18,2) COMMENT '退款金额，按退款发生日',
  stat_time     TIMESTAMP     COMMENT '跑批时间'
) COMMENT '天级交易聚合（事件时间口径）'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/dws.db/trade_1d';

-- ---------------------------------------------------------------- ADS
CREATE EXTERNAL TABLE IF NOT EXISTS ads.trade_recon_1d (
  order_cnt    BIGINT        COMMENT '下单笔数',
  pay_cnt      BIGINT        COMMENT '支付笔数',
  pay_user_cnt BIGINT        COMMENT '支付用户数',
  pay_amt      DECIMAL(18,2) COMMENT '支付 GMV',
  refund_amt   DECIMAL(18,2) COMMENT '退款金额，按原订单日回溯（对账口径）',
  stat_time    TIMESTAMP     COMMENT '跑批时间'
) COMMENT '对账口径结果：5 个交易指标'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/ads.db/trade_recon_1d';
