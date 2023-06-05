from datetime import datetime
from copy import copy
from typing import Callable, Dict, Set

from tzlocal import get_localzone

from vnpy.event import Event, EventEngine
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest,
)
from vnpy.trader.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.utility import ZoneInfo
from vnpy_websocket import WebsocketClient


# 本地时区
LOCAL_TZ: ZoneInfo = get_localzone()

# 实盘和模拟盘Websocket API地址
REAL_WEBSOCKET_HOST: str = "wss://www.deribit.com/ws/api/v2"
TEST_WEBSOCKET_HOST: str = "wss://test.deribit.com/ws/api/v2"

# 委托状态映射
STATUS_DERIBIT2VT: Dict[str, Status] = {
    "open": Status.NOTTRADED,
    "filled": Status.ALLTRADED,
    "rejected": Status.REJECTED,
    "cancelled": Status.CANCELLED,
}

# 委托类型映射
ORDERTYPE_VT2DERIBIT: Dict[OrderType, str] = {
    OrderType.LIMIT: "limit",
    OrderType.MARKET: "market",
    OrderType.STOP: "stop_market"
}
ORDERTYPE_DERIBIT2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2DERIBIT.items()}

# 买卖方向映射
DIRECTION_VT2DERIBIT: Dict[Direction, str] = {
    Direction.LONG: "buy",
    Direction.SHORT: "sell"
}
DIRECTION_DERIBIT2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2DERIBIT.items()}

# 产品类型映射
PRODUCT_DERIBIT2VT: Dict[str, Product] = {
    "spot": Product.SPOT,
    "future": Product.FUTURES,
    "future_combo": Product.FUTURES,
    "option": Product.OPTION,
    "strike": Product.OPTION,
    "option_combo": Product.OPTION
}

# 期权类型映射
OPTIONTYPE_DERIBIT2VT: Dict[str, OptionType] = {
    "call": OptionType.CALL,
    "put": OptionType.PUT
}


class DeribitGateway(BaseGateway):
    """
    vn.py用于对接Deribit交易所的交易接口。
    """

    default_name = "DERIBIT"

    default_setting = {
        "key": "",
        "secret": "",
        "代理地址": "",
        "代理端口": "",
        "服务器": ["REAL", "TEST"]
    }

    exchanges = [Exchange.DERIBIT]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "DERIBIT") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.ws_api: "DeribitWebsocketApi" = DeribitWebsocketApi(self)

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["key"]
        secret: str = setting["secret"]
        proxy_host: str = setting["代理地址"]
        proxy_port: str = setting["代理端口"]
        server: str = setting["服务器"]

        if proxy_port.isdigit():
            proxy_port = int(proxy_port)
        else:
            proxy_port = 0

        self.ws_api.connect(
            key,
            secret,
            proxy_host,
            proxy_port,
            server
        )

        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.ws_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        return self.ws_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.ws_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        pass

    def close(self) -> None:
        """关闭连接"""
        self.ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """处理定时事件"""
        self.query_account()


class DeribitWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway: DeribitGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: DeribitGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.reqid: int = 0
        self.reqid_callback_map: Dict[str, callable] = {}
        self.reqid_currency_map: Dict[str, str] = {}
        self.reqid_order_map: Dict[int, OrderData] = {}

        self.connect_time: int = 0
        self.order_count: int = 1000000
        self.localids: Set[str] = set()

        self.ticks: Dict[str, TickData] = {}

        self.callbacks: Dict[str, callable] = {
            "ticker": self.on_ticker,
            "book": self.on_orderbook,
            "user": self.on_user_update,
        }

    def connect(
        self,
        key: str,
        secret: str,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        """连接服务器"""
        self.key = key
        self.secret = secret

        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        if server == "REAL":
            self.init(REAL_WEBSOCKET_HOST, proxy_host, proxy_port)
        else:
            self.init(TEST_WEBSOCKET_HOST, proxy_host, proxy_port)

        self.start()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        symbol: str = req.symbol

        # 过滤重复的订阅
        if symbol in self.ticks:
            return

        # 创建TICK对象
        tick: TickData = TickData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            name=symbol,
            exchange=Exchange.DERIBIT,
            datetime=datetime.now(LOCAL_TZ),
        )
        self.ticks[symbol] = tick

        # 发出订阅请求
        params: dict = {
            "channels": [
                f"ticker.{symbol}.100ms",
                f"book.{symbol}.none.10.100ms"
            ]
        }

        self.send_request("public/subscribe", params)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        # 检查委托类型是否正确
        if req.type not in ORDERTYPE_VT2DERIBIT:
            self.gateway.write_log(f"委托失败，不支持的委托类型：{req.type.value}")
            return

        # 生成本地委托号
        self.order_count += 1
        orderid: str = str(self.connect_time + self.order_count)
        self.localids.add(orderid)

        side: str = DIRECTION_VT2DERIBIT[req.direction]
        method: str = "private/" + side

        # 生成委托请求
        params: dict = {
            "instrument_name": req.symbol,
            "amount": req.volume,
            "type": ORDERTYPE_VT2DERIBIT[req.type],
            "label": orderid,
            "price": req.price
        }

        reqid: str = self.send_request(
            method,
            params,
            self.on_send_order
        )

        # 推送委托提交中状态
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        self.reqid_order_map[reqid] = order

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        # 如果委托号为本地委托号
        if req.orderid in self.localids:
            self.send_request(
                "private/cancel_by_label",
                {"label": req.orderid},
                self.on_cancel_order
            )
        # 如果委托号为系统委托号
        else:
            self.send_request(
                "private/cancel",
                {"order_id": req.orderid},
                self.on_cancel_order
            )

    def set_heartbeat(self) -> None:
        """设置心跳周期"""
        self.send_request("public/set_heartbeat", {"interval": 10})

    def test_request(self) -> None:
        """测试联通性的请求"""
        self.send_request("/public/test", {})

    def get_access_token(self) -> None:
        """获取访问令牌"""
        params: dict = {
            "grant_type": "client_credentials",
            "client_id": self.key,
            "client_secret": self.secret
        }

        self.send_request(
            "public/auth",
            params,
            self.on_access_token
        )

    def query_instrument(self) -> None:
        """查询合约信息"""
        for currency in ["BTC", "ETH"]:
            params: dict = {
                "currency": currency,
                "expired": False,
            }

            self.send_request(
                "public/get_instruments",
                params,
                self.on_query_instrument
            )

    def query_account(self) -> None:
        """查询资金"""
        for currency in ["BTC", "ETH", "USDC"]:
            params: dict = {"currency": currency}

            self.send_request(
                "private/get_account_summary",
                params,
                self.on_query_account
            )

    def subscribe_topic(self) -> None:
        """订阅委托和成交回报"""
        params: dict = {"channels": ["user.changes.any.any.raw"]}
        self.send_request("private/subscribe", params)

    def query_position(self) -> None:
        """查询持仓"""
        for currency in ["BTC", "ETH"]:
            params: dict = {"currency": currency}

            self.send_request(
                "private/get_positions",
                params,
                self.on_query_position
            )

    def query_order(self) -> None:
        """查询未成交委托"""
        for currency in ["BTC", "ETH"]:
            params: dict = {"currency": currency}

            self.send_request(
                "private/get_open_orders_by_currency",
                params,
                self.on_query_order
            )

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("服务器连接成功")

        self.get_access_token()
        self.query_instrument()

        # 重新订阅之前已订阅的行情
        channels: list = []
        for symbol in self.ticks.keys():
            channels.append(f"ticker.{symbol}.100ms")
            channels.append(f"book.{symbol}.none.10.100ms")

        params: dict = {"channels": channels}
        self.send_request("public/subscribe", params)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("服务器连接断开")

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        # 请求回报
        if "id" in packet:
            packet_id: int = packet["id"]

            if packet_id in self.reqid_callback_map.keys():
                callback: callable = self.reqid_callback_map[packet_id]
                callback(packet)
        # 订阅推送
        elif "params" in packet:
            params: dict = packet["params"]

            # 真实数据推送
            if "channel" in params:
                channel: str = params["channel"]
                kind: str = channel.split(".")[0]
                callback: callable = self.callbacks[kind]
                callback(packet)
            # 连接心跳推送
            elif "type" in params:
                type_: str = params["type"]

                # 响应心跳回复
                if type_ == "test_request":
                    self.test_request()

    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """触发异常回调"""
        msg: str = self.exception_detail(exception_type, exception_value, tb)
        self.gateway.write_log(f"触发异常：{msg}")
        print(msg)

    def on_access_token(self, packet: dict) -> None:
        """登录请求回报"""
        error: dict = packet.get("error", None)
        if error:
            code: int = error["code"]
            msg: str = error["message"]
            self.gateway.write_log(f"服务器登录失败，状态码：{code}, 信息：{msg}")
            return

        self.gateway.write_log("服务器登录成功")

        self.set_heartbeat()
        self.subscribe_topic()
        self.query_position()
        self.query_account()
        self.query_order()

    def on_query_instrument(self, packet: dict) -> None:
        """合约查询回报"""
        error: dict = packet.get("error", None)
        if error:
            msg: str = error["message"]
            self.gateway.write_log(f"合约查询失败，信息：{msg}")
            return

        currency: str = self.reqid_currency_map[packet["id"]]

        for d in packet["result"]:
            contract: ContractData = ContractData(
                symbol=d["instrument_name"],
                exchange=Exchange.DERIBIT,
                name=d["instrument_name"],
                product=PRODUCT_DERIBIT2VT[d["kind"]],
                pricetick=d["tick_size"],
                size=d["contract_size"],
                min_volume=d["min_trade_amount"],
                net_position=True,
                history_data=False,
                gateway_name=self.gateway_name,
            )

            if contract.product == Product.OPTION:
                option_expiry = datetime.fromtimestamp(
                    d["expiration_timestamp"] / 1000
                )
                option_underlying = "_".join([
                    d["base_currency"],
                    option_expiry.strftime("%Y%m%d")
                ])

                contract.option_portfolio = d["base_currency"]
                if "strike" in d:
                    contract.option_strike = d["strike"]
                    contract.option_index = str(d["strike"])
                contract.option_underlying = option_underlying
                if "option_type" in d:
                    contract.option_type = OPTIONTYPE_DERIBIT2VT[d["option_type"]]
                contract.option_expiry = option_expiry

            self.gateway.on_contract(contract)

        self.gateway.write_log(f"{currency}合约信息查询成功")

    def on_query_position(self, packet: dict) -> None:
        """持仓查询回报"""
        error: dict = packet.get("error", None)
        if error:
            msg: str = error["message"]
            code: int = error["code"]
            self.gateway.write_log(f"持仓查询失败，状态码：{code}, 信息：{msg}")
            return

        data: list = packet["result"]
        currency: str = self.reqid_currency_map[packet["id"]]

        for pos in data:
            position: PositionData = PositionData(
                symbol=pos["instrument_name"],
                exchange=Exchange.DERIBIT,
                direction=Direction.NET,
                volume=pos["size"],
                pnl=pos["total_profit_loss"],
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

        self.gateway.write_log(f"{currency}持仓查询成功")

    def on_query_account(self, packet: dict) -> None:
        """资金查询回报"""
        error: dict = packet.get("error", None)
        if error:
            code: int = error["code"]
            msg: str = error["message"]
            self.gateway.write_log(f"资金查询失败，状态码：{code}, 信息：{msg}")
            return

        data: dict = packet["result"]
        currency: str = data["currency"]

        account: AccountData = AccountData(
            accountid=currency,
            balance=data["balance"],
            frozen=data["balance"] - data["available_funds"],
            gateway_name=self.gateway_name,
        )
        self.gateway.on_account(account)

    def on_query_order(self, packet: dict) -> None:
        """未成交委托查询回报"""
        error: dict = packet.get("error", None)
        if error:
            msg: str = error["message"]
            code: int = error["code"]
            self.gateway.write_log(f"未成交委托查询失败，状态码：{code}, 信息：{msg}")
            return

        data: list = packet["result"]
        currency: str = self.reqid_currency_map[packet["id"]]

        for d in data:
            self.on_order(d)

        self.gateway.write_log(f"{currency}委托查询成功")

    def on_send_order(self, packet: dict) -> None:
        """委托下单回报"""
        error: dict = packet.get("error", None)
        if not error:
            return

        msg: str = error["message"]
        code: int = error["code"]

        self.gateway.write_log(
            f"委托失败，代码：{code}，信息：{msg}"
        )

        order: OrderData = self.reqid_order_map[packet["id"]]
        order.status = Status.REJECTED
        self.gateway.on_order(order)

    def on_cancel_order(self, packet: dict) -> None:
        """委托撤单回报"""
        error: dict = packet.get("error", None)
        if error:
            msg: str = error["message"]
            self.gateway.write_log(f"撤单失败，信息：{msg}")

    def on_user_update(self, packet: dict) -> None:
        """用户更新推送"""
        data: dict = packet["params"]["data"]

        for order in data["orders"]:
            self.on_order(order)

        for trade in data["trades"]:
            self.on_trade(trade)

        for position in data["positions"]:
            self.on_position(position)

    def on_order(self, data: dict) -> None:
        """委托更新推送"""
        if data["order_type"] not in ORDERTYPE_DERIBIT2VT:
            self.gateway.write_log(f"收到不支持的类型委托推送{data}")
            return

        if data["label"]:
            orderid: str = data["label"]
            self.localids.add(orderid)
        else:
            orderid: str = data["order_id"]

        price: float = data.get("price", 0)

        order: OrderData = OrderData(
            symbol=data["instrument_name"],
            exchange=Exchange.DERIBIT,
            type=ORDERTYPE_DERIBIT2VT[data["order_type"]],
            orderid=orderid,
            direction=DIRECTION_DERIBIT2VT[data["direction"]],
            price=price,
            volume=data["amount"],
            traded=data["filled_amount"],
            datetime=generate_datetime(data["last_update_timestamp"]),
            status=STATUS_DERIBIT2VT[data["order_state"]],
            gateway_name=self.gateway_name
        )

        self.gateway.on_order(order)

    def on_trade(self, data: dict) -> None:
        """成交更新推送"""
        sys_id: str = data["order_id"]
        local_id: str = sys_id

        trade: TradeData = TradeData(
            symbol=data["instrument_name"],
            exchange=Exchange.DERIBIT,
            orderid=local_id,
            tradeid=data["trade_id"],
            direction=DIRECTION_DERIBIT2VT[data["direction"]],
            price=data["price"],
            volume=data["amount"],
            datetime=generate_datetime(data["timestamp"]),
            gateway_name=self.gateway_name,
        )

        self.gateway.on_trade(trade)

    def on_position(self, data: dict) -> None:
        """持仓更新推送"""
        pos: PositionData = PositionData(
            symbol=data["instrument_name"],
            exchange=Exchange.DERIBIT,
            direction=Direction.NET,
            volume=data["size"],
            price=data["average_price"],
            pnl=data["floating_profit_loss"],
            gateway_name=self.gateway_name,
        )

        self.gateway.on_position(pos)

    def on_ticker(self, packet: dict) -> None:
        """行情推送回报"""
        data: dict = packet["params"]["data"]

        symbol: str = data["instrument_name"]
        tick: TickData = self.ticks[symbol]

        tick.last_price = get_float(data["last_price"])
        tick.high_price = get_float(data["stats"]["high"])
        tick.low_price = get_float(data["stats"]["low"])
        tick.volume = get_float(data["stats"]["volume"])
        tick.open_interest = get_float(data.get("open_interest", 0))
        tick.datetime = generate_datetime(data["timestamp"])
        tick.localtime = datetime.now()

        if 'mark_iv' in data:
            tick.extra = {
                "implied_volatility": get_float(data["mark_iv"]),
                "und_price": get_float(data["underlying_price"]),
                "option_price": get_float(data["mark_price"]),
                "delta": get_float(data["greeks"]["delta"]),
                "gamma": get_float(data["greeks"]["gamma"]),
                "vega": get_float(data["greeks"]["vega"]),
                "theta": get_float(data["greeks"]["theta"]),
            }

        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def on_orderbook(self, packet: dict) -> None:
        """盘口推送回报"""
        data: dict = packet["params"]["data"]

        symbol: str = data["instrument_name"]
        bids: list = data["bids"]
        asks: list = data["asks"]
        tick: TickData = self.ticks[symbol]

        for i in range(min(len(bids), 5)):
            ix = i + 1
            bp, bv = bids[i]
            setattr(tick, f"bid_price_{ix}", bp)
            setattr(tick, f"bid_volume_{ix}", bv)

        for i in range(min(len(asks), 5)):
            ix = i + 1
            ap, av = asks[i]
            setattr(tick, f"ask_price_{ix}", ap)
            setattr(tick, f"ask_volume_{ix}", av)

        tick.datetime = generate_datetime(data["timestamp"])
        tick.localtime = datetime.now()

        self.gateway.on_tick(copy(tick))

    def send_request(
        self,
        method: str,
        params: dict,
        callback: Callable = None
    ) -> int:
        """发送请求"""
        self.reqid += 1

        msg: dict = {
            "jsonrpc": "2.0",
            "id": self.reqid,
            "method": method,
            "params": params
        }

        self.send_packet(msg)

        if callback:
            self.reqid_callback_map[self.reqid] = callback

        if "currency" in params:
            self.reqid_currency_map[self.reqid] = params["currency"]

        return self.reqid


def generate_datetime(timestamp: int) -> datetime:
    """生成时间戳"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    return dt.replace(tzinfo=LOCAL_TZ)


def get_float(value: any) -> float:
    """获取浮点数"""
    if isinstance(value, float):
        return value
    return 0
