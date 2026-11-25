-- 电商交易实时数仓：MySQL 业务库初始化
-- 对应 docs/05-数据源与造数.md 2.1 / 2.2 / 2.3 / 7.2
-- 执行：mysql -uroot -p < 01_init_trade.sql

CREATE DATABASE IF NOT EXISTS trade
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

USE trade;

-- ---------- 维表 ----------
CREATE TABLE IF NOT EXISTS shop_info (
  shop_id     BIGINT      NOT NULL COMMENT '店铺ID',
  shop_name   VARCHAR(64) NOT NULL COMMENT '店铺名',
  shop_type   VARCHAR(16) NOT NULL COMMENT '旗舰店/专卖店/专营店',
  province    VARCHAR(32) NOT NULL COMMENT '省份',
  city        VARCHAR(32) NOT NULL COMMENT '城市',
  create_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (shop_id),
  KEY idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='店铺维表';

CREATE TABLE IF NOT EXISTS user_info (
  user_id       BIGINT      NOT NULL COMMENT '用户ID',
  gender        TINYINT     NOT NULL COMMENT '1男 2女 0未知',
  age           TINYINT     NOT NULL COMMENT '年龄',
  age_range     VARCHAR(8)  NOT NULL COMMENT '18-24 / 25-34 / 35-44 / 45+',
  city          VARCHAR(32) NOT NULL COMMENT '城市',
  user_level    TINYINT     NOT NULL COMMENT '1-5 会员等级',
  register_time DATETIME(3) NOT NULL COMMENT '注册时间',
  update_time   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (user_id),
  KEY idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='用户维表';

CREATE TABLE IF NOT EXISTS product_info (
  product_id    BIGINT        NOT NULL COMMENT '商品ID',
  product_name  VARCHAR(128)  NOT NULL COMMENT '商品名',
  category_id   BIGINT        NOT NULL COMMENT '一级类目ID',
  category_name VARCHAR(32)   NOT NULL COMMENT '一级类目名',
  brand         VARCHAR(32)   NOT NULL COMMENT '品牌',
  price         DECIMAL(10,2) NOT NULL COMMENT '当前售价',
  status        TINYINT       NOT NULL DEFAULT 1 COMMENT '1上架 0下架',
  update_time   DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (product_id),
  KEY idx_update_time (update_time),
  KEY idx_category (category_id)
) ENGINE=InnoDB COMMENT='商品维表';

-- ---------- 事实表 ----------
CREATE TABLE IF NOT EXISTS order_info (
  order_id     BIGINT        NOT NULL COMMENT '订单ID',
  user_id      BIGINT        NOT NULL COMMENT '用户ID',
  shop_id      BIGINT        NOT NULL COMMENT '店铺ID',
  order_amount DECIMAL(12,2) NOT NULL COMMENT '下单金额',
  item_cnt     INT           NOT NULL COMMENT '商品件数',
  order_status SMALLINT      NOT NULL COMMENT '100待支付 200已支付 300已发货 400已签收 500已完成 600已取消',
  is_deleted   TINYINT       NOT NULL DEFAULT 0 COMMENT '逻辑删除',
  create_time  DATETIME(3)   NOT NULL COMMENT '下单时间',
  update_time  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id),
  KEY idx_update_time (update_time),
  KEY idx_create_time (create_time)
) ENGINE=InnoDB COMMENT='订单主表';

CREATE TABLE IF NOT EXISTS order_detail (
  detail_id   BIGINT        NOT NULL AUTO_INCREMENT,
  order_id    BIGINT        NOT NULL COMMENT '订单ID',
  product_id  BIGINT        NOT NULL COMMENT '商品ID',
  num         INT           NOT NULL COMMENT '数量',
  price       DECIMAL(10,2) NOT NULL COMMENT '成交单价',
  amount      DECIMAL(12,2) NOT NULL COMMENT '小计 = num * price',
  create_time DATETIME(3)   NOT NULL,
  PRIMARY KEY (detail_id),
  KEY idx_order_id (order_id),
  KEY idx_create_time (create_time)
) ENGINE=InnoDB COMMENT='订单明细表';

CREATE TABLE IF NOT EXISTS payment_info (
  pay_id      BIGINT        NOT NULL COMMENT '支付流水号',
  order_id    BIGINT        NOT NULL COMMENT '订单ID',
  pay_amount  DECIMAL(12,2) NOT NULL COMMENT '支付金额',
  pay_channel VARCHAR(16)   NOT NULL COMMENT 'alipay/wechat/card',
  pay_status  TINYINT       NOT NULL COMMENT '1成功 2失败 3退款中',
  pay_time    DATETIME(3)   NOT NULL COMMENT '支付时间',
  update_time DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (pay_id),
  KEY idx_order_id (order_id),
  KEY idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='支付流水表';

CREATE TABLE IF NOT EXISTS refund_info (
  refund_id     BIGINT        NOT NULL COMMENT '退款单号',
  order_id      BIGINT        NOT NULL COMMENT '订单ID',
  refund_amount DECIMAL(12,2) NOT NULL COMMENT '退款金额',
  refund_status TINYINT       NOT NULL COMMENT '0申请 1成功 2处理中 3拒绝',
  refund_reason VARCHAR(64)   NOT NULL COMMENT '退款原因',
  refund_time   DATETIME(3)   NOT NULL COMMENT '退款时间',
  update_time   DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (refund_id),
  KEY idx_order_id (order_id),
  KEY idx_update_time (update_time)
) ENGINE=InnoDB COMMENT='退款流水表';

-- ---------- CDC 账号 ----------
-- 密码用占位符，不把真实口令提交进仓库。执行方式：
--   sed "s/__CDC_PASSWORD__/本地口令/g" sql/01_init_trade.sql | mysql -uroot -p
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED BY '__CDC_PASSWORD__';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
