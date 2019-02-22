/*
 Navicat Premium Data Transfer

 Source Server         : 10.0.30.59
 Source Server Type    : MySQL
 Source Server Version : 50632
 Source Host           : 10.0.30.59:3306
 Source Schema         : fof

 Target Server Type    : MySQL
 Target Server Version : 50632
 File Encoding         : 65001

 Date: 27/05/2019 16:54:14
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for fof_market_cycle
-- ----------------------------
DROP TABLE IF EXISTS `fof_market_cycle`;
CREATE TABLE `fof_market_cycle`  (
  `OBJECT_ID` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '对象ID',
  `MARKET_TYPE` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行情类型(G为股市, Z为债市)',
  `CYCLE_TYPE` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行情类型(D为大波段, X为小波段)',
  `STARTDATE` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '开始日期',
  `ENDDATE` varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '结束日期',
  `MARKET_FEATURE` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '行情性质(牛市,熊市,震荡上行,震荡下行)',
  `CREATE_USER_ID` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '创建人',
  `CREATE_TIME` datetime(0) NULL DEFAULT NULL COMMENT '创建日期',
  `UPDATE_USER_ID` varchar(30) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '修改人',
  `UPDATE_TIME` datetime(0) NULL DEFAULT NULL COMMENT '修改日期',
  `DELETE_FLAG` tinyint(1) NULL DEFAULT 0 COMMENT '废除标记',
  PRIMARY KEY (`OBJECT_ID`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '行情切片' ROW_FORMAT = Compact;

SET FOREIGN_KEY_CHECKS = 1;
