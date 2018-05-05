import pandas as pd

from futuquant.open_context import *
from .trade_query_cn import *
from .constant_cn import *


class CNTradeOrderHandlerBase(RspHandlerBase):
    """Base class for handle trader order push"""

    def on_recv_rsp(self, rsp_str):
        """receive response callback function"""
        ret_code, msg, order_info = TradePushQueryCN.cn_unpack_order_push_rsp(rsp_str)
        order_list = [order_info]

        if ret_code == RET_ERROR:
            return ret_code, msg
        else:
            col_list = ['envtype', 'code', 'stock_name', 'dealt_avg_price', 'dealt_qty',
                        'qty', 'orderid', 'order_type',
                        'order_side', 'price', 'status', 'submited_time', 'updated_time'
                        ]

            trade_frame_table = pd.DataFrame(order_list, columns=col_list)

            return RET_OK, trade_frame_table

    def on_error(self, error_str):
        """error callback function"""
        return error_str


class CNTradeOrderPreHandler(RspHandlerBase):
    """class for pre handle trader order push"""

    def __init__(self, notify_obj=None):
        self._notify_obj = notify_obj
        super(CNTradeOrderPreHandler, self).__init__()

    def on_recv_rsp(self, rsp_str):
        """receive response callback function"""
        ret_code, msg, order_info = TradePushQueryCN.cn_unpack_order_push_rsp(rsp_str)

        if ret_code == RET_OK:
            orderid = order_info['orderid']
            envtype = order_info['envtype']
            status = order_info['status']
            if self._notify_obj is not None:
                self._notify_obj.on_trade_order_check(orderid, envtype, status)

        return ret_code, None


class OpenCNTradeContext(OpenContextBase):
    """自定义A股交易接口，这里使用easytrader作为实盘交易接口，统一采用FUTU API定义的通信结构进行交互
    不一样的在于A股的Protocol 为500x，参见trade_query_cn.py
    """
    cookie = 100000

    def __init__(self, host="127.0.0.1", port=11111):
        self._ctx_unlock = None
        self._obj_order_sub = SafeTradeSubscribeList()

        super(OpenCNTradeContext, self).__init__(host, port, True, True)
        self.set_pre_handler(CNTradeOrderPreHandler(self))

    def close(self):
        """
        to call close old obj before loop create new, otherwise socket will encounter erro 10053 or more!
        """
        super(OpenCNTradeContext, self).close()

    def on_api_socket_reconnected(self):
        """for API socket reconnected"""
        # auto unlock
        if self._ctx_unlock is not None:
            for i in range(3):
                password, password_md5 = self._ctx_unlock
                ret, data = self.unlock_trade(password, password_md5)
                if ret == RET_OK:
                    break
                sleep(1)

        # auto subscribe order deal push
        list_sub = self._obj_order_sub.copy()
        dic_order = {}
        list_zero_order_env = []
        for (orderid, envtype) in list_sub:
            if str(orderid) == u'':
                list_zero_order_env.append(envtype)
                continue
            if envtype not in dic_order:
                dic_order[envtype] = []
            dic_order[envtype].append(orderid)

        for envtype in dic_order:
            self._subscribe_order_deal_push(dic_order[envtype], True, True, envtype)

        # use orderid blank to subscrible all order
        for envtype in list_zero_order_env:
            self._subscribe_order_deal_push([], True, False, envtype)

    def on_trade_order_check(self, orderid, envtype, status):
        """multi thread notify order finish after subscribe order push"""
        if is_cntrade_order_status_finish(status):
            self._obj_order_sub.del_val(orderid=orderid, envtype=envtype)
        elif (not self._obj_order_sub.has_val(orderid, envtype)) and self._obj_order_sub.has_val(u'', envtype):
            self._obj_order_sub.add_val(orderid, envtype)  # record info for subscribe order u''

    def _subscribe_order_deal_push(self, orderid_list, order_deal_push=True, push_atonce=True, envtype=0):
        """subscribe order for recv push data"""
        for orderid in orderid_list:
            if order_deal_push is False:
                self._obj_order_sub.del_val(orderid, envtype)
            else:
                self._obj_order_sub.add_val(orderid, envtype)

        ret_code, _, push_req_str = TradePushQuery.cn_pack_subscribe_req(
            str(self.cookie), str(envtype), orderid_list, str(int(order_deal_push)), str(int(push_atonce)))
        if ret_code == RET_OK:
            ret_code, _ = self._send_async_req(push_req_str)

        return ret_code

    def unlock_trade(self, password, password_md5=None):
        """
        交易解锁，安全考虑，所有的交易api,需成功解锁后才可操作
        :param password: 明文密码字符串 (二选一）
        :param password_md5: 密码的md5字符串（二选一）
        :return:(ret, data) ret == 0 时, data为None
                            ret != 0 时， data为错误字符串
        """
        query_processor = self._get_sync_query_processor(UnlockTrade.pack_req,
                                                         UnlockTrade.unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'password': str(password) if password else '',
                 'password_md5': str(password_md5) if password_md5 else ''}

        ret_code, msg, unlock_list = query_processor(**kargs)
        if ret_code != RET_OK:
            return RET_ERROR, msg

        # reconnected to auto unlock
        if RET_OK == ret_code:
            self._ctx_unlock = (password, password_md5)

            # unlock push socket
            ret_code, msg, push_req_str = UnlockTrade.pack_req(**kargs)
            if ret_code == RET_OK:
                self._send_async_req(push_req_str)

        return RET_OK, None

    def subscribe_order_deal_push(self, orderid_list, order_deal_push=True, envtype=0):
        """
        subscribe_order_deal_push
        """
        if not TRADE_CN.check_envtype_cn(envtype):
            return RET_ERROR

        list_sub = [u'']
        if orderid_list is None:
            list_sub = [u'']
        elif isinstance(orderid_list, list):
            list_sub = [str(x) for x in orderid_list]
        else:
            list_sub = [str(orderid_list)]

        return self._subscribe_order_deal_push(list_sub, order_deal_push, True, envtype)

    def place_order(self, price, qty, strcode, orderside, ordertype=0, envtype=0, order_deal_push=False,
                    price_mode=PriceRegularMode.IGNORE):
        """
        place order
        use  set_handle(CNTradeOrderHandlerBase) to recv order push !
        """
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        ret_code, content = split_stock_str(str(strcode))
        if ret_code == RET_ERROR:
            error_str = content
            return RET_ERROR, error_str, None

        market_code, stock_code = content
        if int(market_code) != 1:
            error_str = ERROR_STR_PREFIX + "the type of stocks is wrong "
            return RET_ERROR, error_str

        query_processor = self._get_sync_query_processor(PlaceOrderCN.cn_pack_req,
                                                         PlaceOrderCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'envtype': str(envtype), 'orderside': str(orderside),
                 'ordertype': str(ordertype), 'price': str(price), 'qty': str(qty), 'strcode': str(stock_code),
                 'price_mode': str(price_mode)}

        ret_code, msg, place_order_list = query_processor(**kargs)
        if ret_code != RET_OK:
            return RET_ERROR, msg

        # handle order push
        self._subscribe_order_deal_push(orderid_list=[place_order_list[0]['orderid']],
                                        order_deal_push=order_deal_push, envtype=envtype)

        col_list = ["envtype", "orderid", "code", "stock_name", "dealt_avg_price", "dealt_qty", "qty",
                    "order_type", "order_side", "price", "status", "submited_time", "updated_time"]

        place_order_table = pd.DataFrame(place_order_list, columns=col_list)

        return RET_OK, place_order_table

    def set_order_status(self, status, orderid=0, envtype=0):
        """for setting the status of order"""
        if int(status) not in TRADE_CN.REV_ORDER_STATUS:
            error_str = ERROR_STR_PREFIX + "the type of status is wrong "
            return RET_ERROR, error_str

        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        query_processor = self._get_sync_query_processor(SetOrderStatusCN.cn_pack_req,
                                                         SetOrderStatusCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'envtype': str(envtype), 'localid': str(0),
                 'orderid': str(orderid), 'status': str(status)}

        ret_code, msg, set_order_list = query_processor(**kargs)
        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ['envtype', 'orderID']
        set_order_table = pd.DataFrame(set_order_list, columns=col_list)

        return RET_OK, set_order_table

    def change_order(self, price, qty, orderid=0, envtype=0):
        """for changing the order"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        query_processor = self._get_sync_query_processor(ChangeOrderCN.cn_pack_req,
                                                         ChangeOrderCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'envtype': str(envtype), 'localid': str(0),
                 'orderid': str(orderid), 'price': str(price), 'qty': str(qty)}

        ret_code, msg, change_order_list = query_processor(**kargs)
        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ['envtype', 'orderID']
        change_order_table = pd.DataFrame(change_order_list, columns=col_list)

        return RET_OK, change_order_table

    def accinfo_query(self, envtype=0):
        """
        query account information
        :param envtype: trading environment parameters,0 means real transaction and 1 means simulation trading
        :return:error return RET_ERROR,msg and ok return RET_OK,ret
        """
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        query_processor = self._get_sync_query_processor(AccInfoQuery.hk_pack_req,
                                                         AccInfoQuery.hk_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'envtype': str(envtype)}

        ret_code, msg, accinfo_list = query_processor(**kargs)
        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ['Power', 'ZCJZ', 'ZQSZ', 'XJJY', 'KQXJ', 'DJZJ', 'ZSJE', 'ZGJDE', 'YYJDE', 'GPBZJ']
        accinfo_frame_table = pd.DataFrame(accinfo_list, columns=col_list)

        return RET_OK, accinfo_frame_table

    def order_list_query(self, orderid="", statusfilter="", strcode='', start='', end='', envtype=0):
        """for querying the order list"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        stock_code = ''
        if strcode != '':
            ret_code, content = split_stock_str(str(strcode))
            if ret_code == RET_ERROR:
                return RET_ERROR, content
            _, stock_code = content

        query_processor = self._get_sync_query_processor(OrderListQueryCN.cn_pack_req,
                                                         OrderListQueryCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie),
                 'orderid': str(orderid),
                 'statusfilter': str(statusfilter),
                 'strcode': str(stock_code),
                 'start': str(start),
                 'end': str(end),
                 'envtype': str(envtype)}
        ret_code, msg, order_list = query_processor(**kargs)

        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ["code", "stock_name", "dealt_avg_price", "dealt_qty", "qty",
                    "orderid", "order_type", "order_side", "price",
                    "status", "submited_time", "updated_time"]

        order_list_table = pd.DataFrame(order_list, columns=col_list)

        return RET_OK, order_list_table

    def position_list_query(self, strcode='', stocktype='', pl_ratio_min='',
                            pl_ratio_max='', envtype=0):
        """for querying the position list"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        stock_code = ''
        if strcode != '':
            ret_code, content = split_stock_str(str(strcode))
            if ret_code == RET_ERROR:
                return RET_ERROR, content
            _, stock_code = content

        query_processor = self._get_sync_query_processor(PositionListQueryCN.cn_pack_req,
                                                         PositionListQueryCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie),
                 'strcode': str(stock_code),
                 'stocktype': str(stocktype),
                 'pl_ratio_min': str(pl_ratio_min),
                 'pl_ratio_max': str(pl_ratio_max),
                 'envtype': str(envtype)}
        ret_code, msg, position_list = query_processor(**kargs)

        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ["code", "stock_name", "qty", "can_sell_qty", "cost_price",
                    "cost_price_valid", "market_val", "nominal_price", "pl_ratio",
                    "pl_ratio_valid", "pl_val", "pl_val_valid", "today_buy_qty",
                    "today_buy_val", "today_pl_val", "today_sell_qty", "today_sell_val"]

        position_list_table = pd.DataFrame(position_list, columns=col_list)

        return RET_OK, position_list_table

    def deal_list_query(self, envtype=0):
        """for querying deal list"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        query_processor = self._get_sync_query_processor(DealListQueryCN.cn_pack_req,
                                                         DealListQueryCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie), 'envtype': str(envtype)}
        ret_code, msg, deal_list = query_processor(**kargs)

        if ret_code != RET_OK:
            return RET_ERROR, msg

        # "orderside" 保留是为了兼容旧版本, 对外文档统一为"order_side"
        col_list = ["code", "stock_name", "dealid", "orderid",
                    "qty", "price", "orderside", "time", "order_side"]

        deal_list_table = pd.DataFrame(deal_list, columns=col_list)

        return RET_OK, deal_list_table

    def history_order_list_query(self, statusfilter='', strcode='', start='', end='', envtype=0):
        """for querying the order list"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        stock_code = ''
        if strcode != '':
            ret_code, content = split_stock_str(str(strcode))
            if ret_code == RET_ERROR:
                return RET_ERROR, content
            _, stock_code = content

        query_processor = self._get_sync_query_processor(HistoryOrderListQueryCN.cn_pack_req,
                                                         HistoryOrderListQueryCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie),
                 'statusfilter': str(statusfilter),
                 'strcode': str(stock_code),
                 'start': str(start),
                 'end': str(end),
                 'envtype': str(envtype)}
        ret_code, msg, order_list = query_processor(**kargs)

        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ["code", "stock_name", "dealt_qty", "qty",
                    "orderid", "order_type", "order_side", "price",
                    "status", "submited_time", "updated_time"]

        order_list_table = pd.DataFrame(order_list, columns=col_list)

        return RET_OK, order_list_table

    def history_deal_list_query(self, strcode, start, end, envtype=0):
        """for querying deal list"""
        if not TRADE_CN.check_envtype_cn(envtype):
            error_str = ERROR_STR_PREFIX + "the type of environment param is wrong "
            return RET_ERROR, error_str

        stock_code = ''
        if strcode != '':
            ret_code, content = split_stock_str(str(strcode))
            if ret_code == RET_ERROR:
                return RET_ERROR, content
            _, stock_code = content

        query_processor = self._get_sync_query_processor(HistoryDealListQueryCN.cn_pack_req,
                                                         HistoryDealListQueryCN.cn_unpack_rsp)

        # the keys of kargs should be corresponding to the actual function arguments
        kargs = {'cookie': str(self.cookie),
                 'strcode': str(stock_code),
                 'start': str(start),
                 'end': str(end),
                 'envtype': str(envtype)}

        ret_code, msg, deal_list = query_processor(**kargs)

        if ret_code != RET_OK:
            return RET_ERROR, msg

        col_list = ["code", "stock_name", "dealid", "orderid", "qty", "price",
                    "order_side", "time", "contra_broker_id", "contra_broker_name"]

        deal_list_table = pd.DataFrame(deal_list, columns=col_list)

        return RET_OK, deal_list_table

    def login_new_account(self, user_id, login_password_md5, trade_password, trade_password_md5=None):
        '''
        自动登陆一个新的牛牛帐号
        :param user_id: 牛牛号
        :param login_password_md5: 新帐号的登陆密码的md5值
        :param trade_password: 新帐号的交易密码
        :param trade_password_md5: 新帐号的交易密码的md5值 (跟交易密码二选一)
        :return:
        '''
        query_processor = self._get_sync_query_processor(LoginNewAccountQuery.pack_req,
                                                         LoginNewAccountQuery.unpack_rsp)

        kargs = {'cookie': str(self.cookie),
                 'user_id': str(user_id),
                 'password_md5': str(login_password_md5)
                 }

        # 切换帐号，必然会断线，故判断ret_code 无意义
        try:
            query_processor(**kargs)
        except Exception as e:
            pass

        # 触发重连等待
        self.get_global_state()

        # 接下来就是解锁交易密码
        ret = RET_OK
        data = ''
        if trade_password or trade_password_md5:
            ret, data = self.unlock_trade(trade_password, trade_password_md5)
        else:
            self._ctx_unlock = None

        return ret, data
