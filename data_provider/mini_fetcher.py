# -*- coding: utf-8 -*-
"""
===================================
MiniFetcher - 备用数据源 (Priority 2)
===================================

数据来源：
1. TinyShare（兼容 Tushare API，使用授权码）
2. MiniShare（实时行情 rt_k_ms）

特点：
- TinyShare: Tushare 兼容替代，使用授权码代替 token
- MiniShare: 实时行情数据，无需积分

优先级策略：
- 如果有 minishare_apikey，优先级更高（因为有实时行情）
- 如果只有 tinyshare_token，优先级与 TushareFetcher 相同

流控策略：
1. 实现"每分钟调用计数器"
2. 超过免费配额（80次/分）时，强制休眠到下一分钟
3. 使用 tenacity 实现指数退避重试
"""

import json as _json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS,is_bse_code, is_st_stock, is_kc_cy_stock, normalize_stock_code
from .realtime_types import UnifiedRealtimeQuote
from src.config import get_config
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


# ETF code prefixes by exchange
_ETF_SH_PREFIXES = ('51', '52', '56', '58')
_ETF_SZ_PREFIXES = ('15', '16', '18')
_ETF_ALL_PREFIXES = _ETF_SH_PREFIXES + _ETF_SZ_PREFIXES


def _is_etf_code(stock_code: str) -> bool:
    """Check if the code is an ETF fund code."""
    code = stock_code.strip().split('.')[0]
    return code.startswith(_ETF_ALL_PREFIXES) and len(code) == 6


def _is_us_code(stock_code: str) -> bool:
    """判断代码是否为美股"""
    code = stock_code.strip().upper()
    return bool(re.match(r'^[A-Z]{1,5}(\.[A-Z])?$', code))


class MiniFetcher(BaseFetcher):
    """
    TinyShare + MiniShare 数据源实现

    优先级：2（如果没有 minishare_apikey）
           1（如果有 minishare_apikey，优先实时行情）

    数据来源：
    - TinyShare: Tushare 兼容 API，使用授权码
    - MiniShare: 实时行情接口 rt_k_ms

    关键策略：
    - 每分钟调用计数器，防止超出配额
    - 超过 80 次/分钟时强制等待
    - 失败后指数退避重试
    - 实时行情优先使用 MiniShare
    """

    name = "MiniFetcher"
    priority = int(os.getenv("MINI_FETCHER_PRIORITY", "2"))

    def __init__(self, rate_limit_per_minute: int = 80):
        """
        初始化 MiniFetcher

        Args:
            rate_limit_per_minute: 每分钟最大请求数（默认80）
        """
        self.rate_limit_per_minute = rate_limit_per_minute
        self._call_count = 0
        self._minute_start: Optional[float] = None
        self._api: Optional[object] = None  # TinyShare API 实例
        self._minishare_api: Optional[object] = None  # MiniShare API 实例

        # 尝试初始化 API
        self._init_api()
        self._init_minishare_api()

        # 根据 API 初始化结果动态调整优先级
        self.priority = self._determine_priority()

    def _init_api(self) -> None:
        """
        初始化 TinyShare API（兼容 Tushare）

        优先级：
        1. tinyshare_token - TinyShare 授权码（Tushare 兼容）
        2. tushare_token - Tushare 官方 Token
        """
        config = get_config()

        # 优先使用 tinyshare_token
        token = config.tinyshare_token or config.tushare_token

        if not token:
            logger.warning("TinyShare Token 未配置，此数据源不可用")
            return

        try:
            # 优先尝试 tinyshare
            try:
                import tinyshare as ts
                ts.set_token(token)
                self._api = ts.pro_api()
                self._api_type = 'tinyshare'
                logger.info("TinyShare API 初始化成功")
            except ImportError:
                # 如果没有 tinyshare 库，尝试 tushare
                import tushare as ts
                ts.set_token(token)
                self._api = ts.pro_api()
                self._api_type = 'tushare'
                logger.info("Tushare API 初始化成功（通过 MiniFetcher）")

            # Patch API endpoint
            self._patch_api_endpoint(token)

        except Exception as e:
            logger.error(f"TinyShare/Tushare API 初始化失败: {e}")
            self._api = None
            self._api_type = None

    def _init_minishare_api(self) -> None:
        """
        初始化 MiniShare API（用于实时行情）
        """
        config = get_config()

        if not config.minishare_apikey:
            logger.debug("MiniShare API Key 未配置，跳过实时行情初始化")
            return

        try:
            import minishare as ms

            self._minishare_api = ms.pro_api(config.minishare_apikey)
            logger.info("MiniShare API 初始化成功")

        except Exception as e:
            logger.error(f"MiniShare API 初始化失败: {e}")
            self._minishare_api = None

    def _patch_api_endpoint(self, token: str) -> None:
        """Patch tushare SDK to use the official api.tushare.pro endpoint.

        注意：TinyShare token 不需要 patch，让 tinyshare 库原生处理。
        只有 Tushare 官方 token 才需要 patch。
        """
        import types

        # 检查 token 类型
        try:
            import tinyshare as ts
            if ts.is_tiny_token(token):
                # TinyShare token - 不需要 patch，让库原生处理
                logger.info("使用 TinyShare token，跳过 endpoint patch（原生处理）")
                return
        except ImportError:
            pass

        # 只有 Tushare token 才需要 patch
        TUSHARE_API_URL = "http://api.tushare.pro"
        _token = token
        _timeout = getattr(self._api, '_DataApi__timeout', 30)

        logger.info(f"使用 Tushare token，patch 到: {TUSHARE_API_URL}")

        def patched_query(self_api, api_name, fields='', **kwargs):
            req_params = {
                'api_name': api_name,
                'token': _token,
                'params': kwargs,
                'fields': fields,
            }
            res = requests.post(TUSHARE_API_URL, json=req_params, timeout=_timeout)
            res.raise_for_status()
            data = res.json()
            if data.get('code', 0) != 0:
                raise Exception(data.get('msg', 'Unknown error'))
            df = pd.DataFrame(data.get('data', []))
            return df

        self._api.query = types.MethodType(patched_query, self._api)

    def _determine_priority(self) -> int:
        """
        根据配置动态确定优先级

        优先级规则：
        - 有 MiniShare API（实时行情）：优先级 -2（最高，优于 TushareFetcher）
        - 只有 TinyShare/Tushare：优先级 2
        - API 初始化失败：优先级 99（不可用）

        注意：优先级数字越小越优先
        """
        if self._minishare_api is not None:
            logger.info("✅ 检测到 MINISHARE_APIKEY，MiniFetcher 优先级提升为最高 (Priority -2)")
            return -2  # 最高优先级（有实时行情）
        if self._api is not None:
            return 2  # 与 TushareFetcher 相同
        return 99  # 不可用

    def is_available(self) -> bool:
        """检查数据源是否可用"""
        return self._api is not None or self._minishare_api is not None

    def _check_rate_limit(self) -> None:
        """检查并处理速率限制"""
        import time

        current_time = time.time()

        # 重置计数器（每分钟）
        if self._minute_start is None or current_time - self._minute_start >= 60:
            self._call_count = 0
            self._minute_start = current_time

        # 超过限制则等待
        if self._call_count >= self.rate_limit_per_minute:
            sleep_time = 60 - (current_time - self._minute_start)
            if sleep_time > 0:
                logger.warning(f"速率限制触发，休眠 {sleep_time:.1f} 秒")
                time.sleep(sleep_time)
                self._call_count = 0
                self._minute_start = time.time()

        self._call_count += 1

    def _convert_stock_code(self, stock_code: str) -> str:
        """转换股票代码为 Tushare 格式"""
        code = normalize_stock_code(stock_code)

        # 判断交易所
        if code.startswith(('688', '689', '8', '9')):  # 科创板、B股
            if len(code) == 6 and code[0] in ('8', '9'):
                return f"{code}.BJ"
            return f"{code}.SH"
        elif code.startswith(('00', '02', '30')):  # 深圳主板、中小板、创业板
            return f"{code}.SZ"
        elif code.startswith(('60', '68', '5', '9')):  # 上海主板、科创板
            return f"{code}.SH"

        # 尝试从代码判断
        if len(code) == 6:
            if code[0] in ('0', '2', '3'):
                return f"{code}.SZ"
            elif code[0] in ('5', '6', '8', '9'):
                return f"{code}.SH"

        return code

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从数据源获取原始数据"""
        if self._api is None:
            raise DataFetchError("TinyShare API 不可用")

        self._check_rate_limit()

        # 转换日期格式
        ts_start = start_date.replace('-', '')
        ts_end = end_date.replace('-', '')

        ts_code = self._convert_stock_code(stock_code)

        try:
            if _is_etf_code(stock_code):
                df = self._api.query(
                    'fund_daily',
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )
            else:
                df = self._api.query(
                    'daily',
                    ts_code=ts_code,
                    start_date=ts_start,
                    end_date=ts_end,
                )

            if df is None or df.empty:
                raise DataFetchError(f"未获取到 {stock_code} 的数据")

            return df

        except Exception as e:
            raise DataFetchError(f"获取 {stock_code} 数据失败: {e}")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """标准化数据列名"""
        if df is None or df.empty:
            return df

        # 统一列名为小写
        df.columns = [col.lower() for col in df.columns]

        # 映射列名
        column_mapping = {
            'trade_date': 'date',
            'ts_code': 'code',
            'pre_close': 'pre_close',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume',
            'volume': 'volume',
            'amount': 'amount',
            'pct_chg': 'pct_chg',
            'change': 'change',
        }

        # 重命名列
        df = df.rename(columns=column_mapping)

        # 确保日期格式正确
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')

        # 填充缺失值
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        return df

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """获取股票名称"""
        if self._api is None:
            return None

        try:
            self._check_rate_limit()

            ts_code = self._convert_stock_code(stock_code)

            if _is_etf_code(stock_code):
                df = self._api.query(
                    'fund_basic',
                    ts_code=ts_code,
                    fields='ts_code,name'
                )
            else:
                df = self._api.query(
                    'stock_basic',
                    ts_code=ts_code,
                    fields='ts_code,name'
                )

            if df is not None and not df.empty:
                return df.iloc[0]['name']

        except Exception as e:
            logger.error(f"获取股票名称失败: {e}")

        return None

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        """获取股票列表"""
        if self._api is None:
            return None

        try:
            self._check_rate_limit()

            df = self._api.query(
                'stock_basic',
                exchange='',
                list_status='L',
                fields='ts_code,name,area,industry,list_date'
            )

            return df

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")

        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[UnifiedRealtimeQuote]:
        """获取实时行情"""
        # 优先使用 MiniShare
        if self._minishare_api is not None:
            try:
                ts_code = self._convert_stock_code(stock_code)
                df = self._minishare_api.rt_k_ms(ts_code=ts_code)

                if df is not None and not df.empty:
                    row = df.iloc[0]
                    return UnifiedRealtimeQuote(
                        code=stock_code,
                        name=row.get('name', ''),
                        price=float(row.get('close', 0)),
                        change_pct=float(row.get('pct_chg', 0)),
                        change_amount=float(row.get('change', 0)),
                        volume=int(row.get('vol', 0)),
                        amount=float(row.get('amount', 0)),
                        turnover_rate=float(row.get('turnover_rate', 0)),
                        open_price=float(row.get('open', 0)),
                        high=float(row.get('high', 0)),
                        low=float(row.get('low', 0)),
                    )
            except Exception as e:
                logger.warning(f"[MiniShare] 获取实时行情失败: {e}")

        # Fallback: 使用 TinyShare/Tushare
        if self._api is None:
            return None

        try:
            self._check_rate_limit()

            ts_code = self._convert_stock_code(stock_code)

            # 尝试使用 daily 接口获取最新行情
            from datetime import datetime, timedelta
            current_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

            df = self._api.query(
                'daily',
                ts_code=ts_code,
                start_date=start_date,
                end_date=current_date,
            )

            if df is not None and not df.empty:
                row = df.iloc[-1]
                return UnifiedRealtimeQuote(
                    code=stock_code,
                    name='',
                    price=float(row.get('close', 0)),
                    change_pct=float(row.get('pct_chg', 0)),
                    change_amount=float(row.get('change', 0)),
                    volume=int(row.get('vol', 0)),
                    amount=float(row.get('amount', 0)),
                    open_price=float(row.get('open', 0)),
                    high=float(row.get('high', 0)),
                    low=float(row.get('low', 0)),
                )

        except Exception as e:
            logger.warning(f"[TinyShare] 获取实时行情失败: {e}")

        return None

    def get_main_indices(self, region: str = "cn") -> Optional[List[dict]]:
        """获取主要指数实时行情"""
        if self._minishare_api is None and self._api is None:
            return None

        try:
            # 指数代码映射
            indices_map = {
                'sh': ('000001.SH', '上证指数'),
                'sz': ('399001.SH', '深证成指'),
                'hs300': ('000300.SH', '沪深300'),
                'cyb': ('399006.SZ', '创业板指'),
                'zxb': ('399005.SZ', '中小板指'),
                'sh50': ('000016.SH', '上证50'),
            }

            if region not in indices_map:
                return None

            ts_code, name = indices_map[region]

            # 优先使用 MiniShare
            if self._minishare_api is not None:
                df = self._minishare_api.rt_k_ms(ts_code=ts_code)
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    return [{
                        'code': ts_code,
                        'name': name,
                        'current': float(row.get('close', 0)),
                        'change': float(row.get('change', 0)),
                        'change_pct': float(row.get('pct_chg', 0)),
                        'volume': int(row.get('vol', 0)),
                        'amount': float(row.get('amount', 0)),
                    }]

            # Fallback: 使用 TinyShare/Tushare
            if self._api is not None:
                self._check_rate_limit()

                from datetime import datetime, timedelta
                current_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

                df = self._api.query(
                    'daily',
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=current_date,
                )

                if df is not None and not df.empty:
                    row = df.iloc[-1]
                    return [{
                        'code': ts_code,
                        'name': name,
                        'current': float(row.get('close', 0)),
                        'change': float(row.get('change', 0)),
                        'change_pct': float(row.get('pct_chg', 0)),
                        'volume': int(row.get('vol', 0)),
                        'amount': float(row.get('amount', 0)),
                    }]

        except Exception as e:
            logger.error(f"获取指数行情失败: {e}")

        return None

    def get_market_stats(self) -> Optional[dict]:
        """
        获取市场涨跌统计

        优先使用 MiniShare rt_k_ms 接口获取实时数据
        """
        # 优先尝试 MiniShare 实时行情
        if self._minishare_api is not None:
            try:
                logger.info("[MiniShare] rt_k_ms 获取市场统计...")
                df = self._minishare_api.rt_k_ms(ts_code='3*.SZ,6*.SH,0*.SZ,9*.BJ')
                if df is not None and not df.empty:
                    return self._calc_market_stats(df)
            except Exception as e:
                logger.warning(f"[MiniShare] rt_k_ms 获取实时数据失败: {e}")

        # Fallback: 使用 TinyShare/Tushare
        if self._api is None:
            return None

        try:
            self._check_rate_limit()
            logger.info("[TinyShare] 获取市场统计...")

            # 获取当前中国时间，判断是否在交易时间内
            china_now = datetime.now(ZoneInfo("Asia/Shanghai"))
            china_now_str = china_now.strftime("%H:%M")
            current_date = china_now.strftime("%Y%m%d")

            start_date = (datetime.now() - pd.Timedelta(days=20)).strftime('%Y%m%d')
            df_cal = self._api.query('trade_cal', exchange='SSE', start_date=start_date, end_date=current_date)

            # 过滤出 is_open == 1 的日期
            date_list = df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()

            if current_date in date_list:
                if china_now_str < '09:30' or china_now_str > '16:30':
                    use_realtime = False
                else:
                    use_realtime = True
            else:
                use_realtime = False

            if use_realtime:
                try:
                    df = self._api.query('rt_k', ts_code='3*.SZ,6*.SH,0*.SZ,92*.BJ')
                    if df is not None and not df.empty:
                        return self._calc_market_stats(df)
                except Exception as e:
                    logger.error(f"[TinyShare] rt_k 尝试获取实时数据失败: {e}")
                    return None
            else:
                if current_date not in date_list:
                    last_date = date_list[0]
                else:
                    if china_now_str < '09:30':
                        last_date = date_list[1]
                    else:
                        last_date = date_list[0]

                try:
                    df = self._api.query('daily', ts_code='3*.SZ,6*.SH,0*.SZ,92*.BJ', start_date=last_date, end_date=last_date)
                    df.columns = [col.lower() for col in df.columns]

                    df_basic = self._api.query('stock_basic', fields='ts_code,name')
                    df = pd.merge(df, df_basic, on='ts_code', how='left')

                    if 'amount' in df.columns:
                        df['amount'] = df['amount'] * 1000

                    if df is not None and not df.empty:
                        return self._calc_market_stats(df)
                except Exception as e:
                    logger.error(f"[TinyShare] daily 获取数据失败: {e}")

        except Exception as e:
            logger.error(f"获取市场统计失败: {e}")

        return None

    def _calc_market_stats(self, df: pd.DataFrame) -> Optional[dict]:
        """计算市场涨跌统计"""
        if df is None or df.empty:
            return None

        try:
            # 确保 pct_chg 列存在
            if 'pct_chg' not in df.columns:
                logger.warning("数据中缺少 pct_chg 列")
                return None

            # 转换涨跌幅为数值
            df['pct_chg'] = pd.to_numeric(df['pct_chg'], errors='coerce').fillna(0)

            # 统计
            up_count = int((df['pct_chg'] > 0).sum())
            down_count = int((df['pct_chg'] < 0).sum())
            flat_count = int((df['pct_chg'] == 0).sum())

            # 涨停跌停统计（A股涨跌幅限制：主板10%，创业板/科创板20%）
            # 使用 pre_close 计算涨跌停价
            if 'pre_close' in df.columns:
                df['pre_close'] = pd.to_numeric(df['pre_close'], errors='coerce')

                # 简单判断：涨跌幅 >= 9.5% 视为涨停（考虑四舍五入）
                limit_up_count = int((df['pct_chg'] >= 9.5).sum())
                # 跌停：涨跌幅 <= -9.5%
                limit_down_count = int((df['pct_chg'] <= -9.5).sum())
            else:
                limit_up_count = 0
                limit_down_count = 0

            # 成交额统计
            total_amount = 0
            if 'amount' in df.columns:
                total_amount = float(pd.to_numeric(df['amount'], errors='coerce').sum())

            return {
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'limit_up_count': limit_up_count,
                'limit_down_count': limit_down_count,
                'total_amount': total_amount,
            }

        except Exception as e:
            logger.error(f"计算市场统计失败: {e}")
            return None

    def get_sector_rankings(self, n: int = 5) -> Optional[Tuple[list, list]]:
        """获取板块涨跌榜"""
        # TinyShare/MiniShare 不直接提供板块数据，返回 None 让其他数据源处理
        return None
