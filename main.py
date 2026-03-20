# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 主调度程序
===================================

职责：
1. 协调各模块完成股票分析流程
2. 实现低并发的线程池调度
3. 全局异常处理，确保单股失败不影响整体
4. 提供命令行入口
"""
import os
from src.config import setup_env
setup_env()

# 代理配置 - 通过 USE_PROXY 环境变量控制，默认关闭
if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
    proxy_port = os.getenv("PROXY_PORT", "10809")
    proxy_url = f"http://{proxy_host}:{proxy_port}"
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from data_provider.base import canonical_stock_code
from src.core.pipeline import StockAnalysisPipeline
from src.core.market_review import run_market_review
from src.webui_frontend import prepare_webui_frontend_assets
from src.config import get_config, Config
from src.logging_config import setup_logging


logger = logging.getLogger(__name__)

# ==========================================
# [我们手动新增的代码] 钉钉自定义推送机器人
# ==========================================
def send_dingtalk_notification(content: str, title: str = "AI 股票分析报告"):
    """自定义的钉钉推送逻辑"""
    access_token = os.environ.get("DINGTALK_ACCESS_TOKEN")
    secret = os.environ.get("DINGTALK_SECRET")
    
    if not access_token or not secret:
        return

    logger.info("正在生成并发送钉钉消息...")
    try:
        import time, hmac, hashlib, base64, urllib.parse, requests, json
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))

        url = f"https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}"
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"### {title}\n\n" + content
            },
            "at": {"isAtAll": False}
        }
        headers = {'Content-Type': 'application/json; charset=utf-8'}
        response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=15)
        res_data = response.json()
        if res_data.get("errcode") == 0:
            logger.info("✅ 钉钉消息发送成功！请去群聊查看。")
        else:
            logger.warning(f"❌ 钉钉消息发送失败: {res_data}")
    except Exception as e:
        logger.error(f"❌ 钉钉发送发生异常: {e}")
# ==========================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='A股自选股智能分析系统')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--stocks', type=str)
    parser.add_argument('--no-notify', action='store_true')
    parser.add_argument('--single-notify', action='store_true')
    parser.add_argument('--workers', type=int, default=None)
    parser.add_argument('--schedule', action='store_true')
    parser.add_argument('--no-run-immediately', action='store_true')
    parser.add_argument('--market-review', action='store_true')
    parser.add_argument('--no-market-review', action='store_true')
    parser.add_argument('--force-run', action='store_true')
    parser.add_argument('--webui', action='store_true')
    parser.add_argument('--webui-only', action='store_true')
    parser.add_argument('--serve', action='store_true')
    parser.add_argument('--serve-only', action='store_true')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--host', type=str, default='0.0.0.0')
    parser.add_argument('--no-context-snapshot', action='store_true')
    parser.add_argument('--backtest', action='store_true')
    parser.add_argument('--backtest-code', type=str, default=None)
    parser.add_argument('--backtest-days', type=int, default=None)
    parser.add_argument('--backtest-force', action='store_true')
    return parser.parse_args()

def _compute_trading_day_filter(config: Config, args: argparse.Namespace, stock_codes: List[str]) -> Tuple[List[str], Optional[str], bool]:
    force_run = getattr(args, 'force_run', False)
    if force_run or not getattr(config, 'trading_day_check_enabled', True):
        return (stock_codes, None, False)

    from src.core.trading_calendar import get_market_for_stock, get_open_markets_today, compute_effective_region
    open_markets = get_open_markets_today()
    filtered_codes = []
    for code in stock_codes:
        mkt = get_market_for_stock(code)
        if mkt in open_markets or mkt is None:
            filtered_codes.append(code)

    if config.market_review_enabled and not getattr(args, 'no_market_review', False):
        effective_region = compute_effective_region(getattr(config, 'market_review_region', 'cn') or 'cn', open_markets)
    else:
        effective_region = None

    should_skip_all = (not filtered_codes) and (effective_region or '') == ''
    return (filtered_codes, effective_region, should_skip_all)

def run_full_analysis(config: Config, args: argparse.Namespace, stock_codes: Optional[List[str]] = None):
    try:
        if stock_codes is None:
            config.refresh_stock_list()

        effective_codes = stock_codes if stock_codes is not None else config.stock_list
        filtered_codes, effective_region, should_skip = _compute_trading_day_filter(config, args, effective_codes)
        
        if should_skip:
            logger.info("今日所有相关市场均为非交易日，跳过执行。可使用 --force-run 强制执行。")
            return
            
        if set(filtered_codes) != set(effective_codes):
            skipped = set(effective_codes) - set(filtered_codes)
            logger.info("今日休市股票已跳过: %s", skipped)
        stock_codes = filtered_codes

        if getattr(args, 'single_notify', False):
            config.single_stock_notify = True

        merge_notification = (
            getattr(config, 'merge_email_notification', False)
            and config.market_review_enabled
            and not getattr(args, 'no_market_review', False)
            and not config.single_stock_notify
        )

        save_context_snapshot = None
        if getattr(args, 'no_context_snapshot', False):
            save_context_snapshot = False
        query_id = uuid.uuid4().hex
        pipeline = StockAnalysisPipeline(
            config=config, max_workers=args.workers, query_id=query_id, 
            query_source="cli", save_context_snapshot=save_context_snapshot
        )

        results = pipeline.run(
            stock_codes=stock_codes, dry_run=args.dry_run, 
            send_notification=not args.no_notify, merge_notification=merge_notification
        )

        analysis_delay = getattr(config, 'analysis_delay', 0)
        if (analysis_delay > 0 and config.market_review_enabled and not args.no_market_review and effective_region != ''):
            logger.info(f"等待 {analysis_delay} 秒后执行大盘复盘（避免API限流）...")
            time.sleep(analysis_delay)

        market_report = ""
        if (config.market_review_enabled and not args.no_market_review and effective_region != ''):
            review_result = run_market_review(
                notifier=pipeline.notifier, analyzer=pipeline.analyzer, search_service=pipeline.search_service,
                send_notification=not args.no_notify, merge_notification=merge_notification, override_region=effective_region,
            )
            if review_result:
                market_report = review_result

        if merge_notification and (results or market_report) and not args.no_notify:
            parts = []
            if market_report:
                parts.append(f"# 📈 大盘复盘\n\n{market_report}")
            if results:
                dashboard_content = pipeline.notifier.generate_aggregate_report(results, getattr(config, 'report_type', 'simple'))
                parts.append(f"# 🚀 个股决策仪表盘\n\n{dashboard_content}")
            if parts:
                combined_content = "\n\n---\n\n".join(parts)
                if pipeline.notifier.is_available():
                    if pipeline.notifier.send(combined_content, email_send_to_all=True):
                        logger.info("已合并推送（个股+大盘复盘）")

        if results:
            logger.info("\n===== 分析结果摘要 =====")
            for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
                emoji = r.get_emoji()
                logger.info(f"{emoji} {r.name}({r.code}): {r.operation_advice} | 评分 {r.sentiment_score} | {r.trend_prediction}")

        logger.info("\n任务执行完成")

        try:
            from src.feishu_doc import FeishuDocManager
            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                tz_cn = timezone(timedelta(hours=8))
                now = datetime.now(tz_cn)
                doc_title = f"{now.strftime('%Y-%m-%d %H:%M')} 大盘复盘"
                full_content = ""
                if market_report:
                    full_content += f"# 📈 大盘复盘\n\n{market_report}\n\n---\n\n"
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(results, getattr(config, 'report_type', 'simple'))
                    full_content += f"# 🚀 个股决策仪表盘\n\n{dashboard_content}"
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url:
                    logger.info(f"飞书云文档创建成功: {doc_url}")
        except Exception as e:
            logger.error(f"飞书文档生成失败: {e}")

        # ==========================================
        # [我们手动新增的代码] 触发钉钉推送
        # ==========================================
        if os.environ.get("DINGTALK_ACCESS_TOKEN") and (results or market_report) and not args.no_notify:
            try:
                dingtalk_content = ""
                if market_report:
                    dingtalk_content += f"# 📈 大盘复盘\n\n{market_report}\n\n---\n\n"
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(results, getattr(config, 'report_type', 'simple'))
                    dingtalk_content += f"# 🚀 个股决策仪表盘\n\n{dashboard_content}"
                
                now_str = datetime.now().strftime('%Y-%m-%d')
                send_dingtalk_notification(dingtalk_content, title=f"{now_str} AI股票分析日报")
            except Exception as e:
                logger.error(f"调用钉钉推送失败: {e}")
        # ==========================================

        try:
            if getattr(config, 'backtest_enabled', False):
                from src.services.backtest_service import BacktestService
                service = BacktestService()
                stats = service.run_backtest(force=False, eval_window_days=getattr(config, 'backtest_eval_window_days', 10), limit=200)
                logger.info("自动回测完成")
        except Exception as e:
            pass

    except Exception as e:
        logger.exception(f"分析流程执行失败: {e}")

def start_api_server(host: str, port: int, config: Config) -> None:
    import threading
    import uvicorn
    def run_server():
        level_name = (config.log_level or "INFO").lower()
        uvicorn.run("api.app:app", host=host, port=port, log_level=level_name, log_config=None)
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

def start_bot_stream_clients(config: Config) -> None:
    pass

def main() -> int:
    args = parse_arguments()
    config = get_config()
    setup_logging(log_prefix="stock_analysis", debug=args.debug, log_dir=config.log_dir)

    stock_codes = None
    if args.stocks:
        stock_codes = [canonical_stock_code(c) for c in args.stocks.split(',') if (c or "").strip()]

    if args.webui: args.serve = True
    if args.webui_only: args.serve_only = True
    if config.webui_enabled and not (args.serve or args.serve_only): args.serve = True

    start_serve = (args.serve or args.serve_only) and os.getenv("GITHUB_ACTIONS") != "true"

    if start_serve:
        if args.host == '0.0.0.0' and os.getenv('WEBUI_HOST'): args.host = os.getenv('WEBUI_HOST')
        if args.port == 8000 and os.getenv('WEBUI_PORT'): args.port = int(os.getenv('WEBUI_PORT'))
        start_api_server(host=args.host, port=args.port, config=config)

    if args.serve_only:
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            return 0

    try:
        if getattr(args, 'backtest', False):
            from src.services.backtest_service import BacktestService
            service = BacktestService()
            service.run_backtest(code=getattr(args, 'backtest_code', None), force=getattr(args, 'backtest_force', False), eval_window_days=getattr(args, 'backtest_days', None))
            return 0

        if args.market_review:
            from src.analyzer import GeminiAnalyzer
            from src.core.market_review import run_market_review
            from src.notification import NotificationService
            from src.search_service import SearchService

            effective_region = None
            if not getattr(args, 'force_run', False) and getattr(config, 'trading_day_check_enabled', True):
                from src.core.trading_calendar import get_open_markets_today, compute_effective_region as _compute_region
                open_markets = get_open_markets_today()
                effective_region = _compute_region(getattr(config, 'market_review_region', 'cn') or 'cn', open_markets)
                if effective_region == '':
                    return 0

            notifier = NotificationService()
            search_service = None
            analyzer = None
            if config.gemini_api_key or config.openai_api_key:
                analyzer = GeminiAnalyzer(api_key=config.gemini_api_key)
            run_market_review(notifier=notifier, analyzer=analyzer, search_service=search_service, send_notification=not args.no_notify, override_region=effective_region)
            return 0

        if args.schedule or config.schedule_enabled:
            should_run_immediately = config.schedule_run_immediately
            if getattr(args, 'no_run_immediately', False):
                should_run_immediately = False

            from src.scheduler import run_with_schedule
            def scheduled_task(): run_full_analysis(config, args, stock_codes)
            run_with_schedule(task=scheduled_task, schedule_time=config.schedule_time, run_immediately=should_run_immediately)
            return 0

        if config.run_immediately:
            run_full_analysis(config, args, stock_codes)

        keep_running = start_serve and not (args.schedule or config.schedule_enabled)
        if keep_running:
            try:
                while True: time.sleep(1)
            except KeyboardInterrupt:
                pass
        return 0

    except KeyboardInterrupt:
        return 130
    except Exception as e:
        return 1

if __name__ == "__main__":
    sys.exit(main())
