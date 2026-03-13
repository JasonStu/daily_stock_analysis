# -*- coding: utf-8 -*-
"""
MiniFetcher API 验证测试

测试目标：
1. 验证 token 是否有效
2. 验证 API 是否能获取股票名称
3. 验证 API 是否能获取历史数据

运行方式：
    pytest tests/test_mini_fetcher_api.py -v -m network
"""

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.mini_fetcher import MiniFetcher

logger = logging.getLogger(__name__)


class TestMiniFetcherTokenValidation:
    """测试 MiniFetcher Token 验证"""

    @pytest.mark.network
    def test_token_is_valid(self):
        """
        测试 Token 是否有效

        预期：API 调用不返回 "token不对" 错误
        如果 token 无效，此测试应该失败
        """
        fetcher = MiniFetcher()

        if not fetcher.is_available():
            pytest.skip("MiniFetcher API 不可用（未配置 token）")

        # 尝试调用 API 获取股票列表
        # 如果 token 无效，会抛出异常或返回错误
        try:
            df = fetcher.get_stock_list()
            # 如果 token 无效，可能返回 None 或空 DataFrame
            # 或者在日志中看到 "token不对" 错误
            if df is None or df.empty:
                pytest.fail("API 返回空数据，可能 token 无效")
            logger.info(f"Token 验证成功，获取到 {len(df)} 只股票")
        except Exception as e:
            error_msg = str(e)
            if "token不对" in error_msg:
                pytest.fail(f"Token 无效: {error_msg}")
            # 其他错误也记录下来
            logger.warning(f"API 调用出现其他错误: {error_msg}")

    @pytest.mark.network
    def test_get_stock_name(self):
        """
        测试获取股票名称功能

        预期：能够成功获取股票名称
        如果 token 无效，此测试应该失败
        """
        fetcher = MiniFetcher()

        if not fetcher.is_available():
            pytest.skip("MiniFetcher API 不可用")

        # 测试获取股票名称
        stock_code = "600519"  # 贵州茅台
        name = fetcher.get_stock_name(stock_code)

        # 验证返回了名称
        if name is None:
            pytest.fail(f"无法获取股票 {stock_code} 的名称 - Token 可能无效")
        assert isinstance(name, str), f"股票名称应该是字符串，实际: {type(name)}"
        assert len(name) > 0, "股票名称不能为空"
        logger.info(f"成功获取股票名称: {stock_code} -> {name}")

    @pytest.mark.network
    def test_get_historical_data(self):
        """
        测试获取历史数据功能

        预期：能够成功获取股票历史 K 线数据
        """
        fetcher = MiniFetcher()

        if not fetcher.is_available():
            pytest.skip("MiniFetcher API 不可用")

        # 测试获取历史数据
        stock_code = "600519"
        df = fetcher.get_daily_data(stock_code, "2025-01-01", "2025-03-01")

        # 验证返回了数据
        assert df is not None, f"无法获取股票 {stock_code} 的历史数据"
        assert not df.empty, f"返回的数据为空"
        assert "close" in df.columns, "数据中应包含 close 列"
        logger.info(f"成功获取历史数据: {stock_code}, {len(df)} 条记录")

    @pytest.mark.network
    def test_get_realtime_quote(self):
        """
        测试获取实时行情功能

        预期：能够成功获取实时行情（如果有 MiniShare API）
        """
        fetcher = MiniFetcher()

        # 无论 API 是否可用，都测试
        # 如果没有配置，会返回 None，这是预期行为
        stock_code = "600519"
        quote = fetcher.get_realtime_quote(stock_code)

        # 如果返回了数据，验证数据格式
        if quote is not None:
            assert quote.code == stock_code, "股票代码不匹配"
            assert quote.price > 0, "价格应该大于 0"
            logger.info(f"成功获取实时行情: {stock_code}, 价格: {quote.price}")
        else:
            logger.info("未获取到实时行情（可能未配置 MiniShare API）")
