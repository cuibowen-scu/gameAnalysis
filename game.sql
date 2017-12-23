-- --------------------------------------------------------
-- 主机:                           127.0.0.1
-- 服务器版本:                        5.5.53 - MySQL Community Server (GPL)
-- 服务器操作系统:                      Win32
-- HeidiSQL 版本:                  9.4.0.5125
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- 导出 game 的数据库结构
CREATE DATABASE IF NOT EXISTS `game` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `game`;

-- 导出  表 game.average_game_time 结构
CREATE TABLE IF NOT EXISTS `average_game_time` (
  `day` varchar(64) DEFAULT NULL,
  `time` double DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.average_game_times 结构
CREATE TABLE IF NOT EXISTS `average_game_times` (
  `day` varchar(64) DEFAULT NULL,
  `times` double DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.d1rr 结构
CREATE TABLE IF NOT EXISTS `d1rr` (
  `day` varchar(64) DEFAULT NULL,
  `rate` double DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='c';

-- 数据导出被取消选择。
-- 导出  表 game.d7rr 结构
CREATE TABLE IF NOT EXISTS `d7rr` (
  `day` varchar(64) DEFAULT NULL,
  `rate` double DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.dau 结构
CREATE TABLE IF NOT EXISTS `dau` (
  `day` varchar(64) DEFAULT NULL,
  `user_num` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.dnu 结构
CREATE TABLE IF NOT EXISTS `dnu` (
  `day` varchar(64) DEFAULT NULL,
  `user_num` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='c';

-- 数据导出被取消选择。
-- 导出  表 game.gender 结构
CREATE TABLE IF NOT EXISTS `gender` (
  `sex` varchar(64) DEFAULT NULL,
  `number` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.province 结构
CREATE TABLE IF NOT EXISTS `province` (
  `name` varchar(64) DEFAULT NULL,
  `user` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
-- 导出  表 game.rank 结构
CREATE TABLE IF NOT EXISTS `rank` (
  `rank_block` varchar(64) DEFAULT NULL,
  `count` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- 数据导出被取消选择。
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
