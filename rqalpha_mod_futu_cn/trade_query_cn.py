# -*- coding: utf-8 -*-
"""
    A股的交易查询相关接口，自定义对接easytrader的客户端，模拟Futu的api。
"""

from futuquant.trade_query import *


# 判断A股订单交易是否完成，协议参考FUTU港股
def is_cntrade_order_status_finish(status):
    val = int(status)
    if val == 3 or val == 5 or val == 6 or val == 7:
        return True
    return False


class PlaceOrderCN(PlaceOrder):
    """Palce order class，和港股不一样的在于Protocol为5003."""

    def __init__(self):
        super(PlaceOrderCN, self).__init__()
        pass

    @classmethod
    def cn_pack_req(cls, cookie, envtype, orderside, ordertype, price, qty, strcode, price_mode):
        """Convert from user request for trading days to PLS request"""
        if int(orderside) < 0 or int(orderside) > 1:
            error_str = ERROR_STR_PREFIX + "parameter orderside is wrong"
            return RET_ERROR, error_str, None

        if int(ordertype) is not 0 and int(ordertype) is not 1 and int(ordertype) is not 3:
            error_str = ERROR_STR_PREFIX + "parameter ordertype is wrong"
            return RET_ERROR, error_str, None

        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5003",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            "OrderSide": orderside,
                            "OrderType": ordertype,
                            "Price": price_to_str_int1000(price),
                            "Qty": qty,
                            "StockCode": strcode,
                            "PriceMode": price_mode
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class SetOrderStatusCN(SetOrderStatus):
    """calss for setting status of order"""

    def __init__(self):
        super(SetOrderStatusCN, self).__init__()
        pass

    @classmethod
    def cn_pack_req(cls, cookie, envtype, localid, orderid, status):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        if int(status) < 0 or int(status) > 3:
            error_str = ERROR_STR_PREFIX + "parameter status is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5004",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            "LocalID": localid,
                            "OrderID": orderid,
                            "SetOrderStatus": status,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class ChangeOrderCN(ChangeOrder):
    """Class for changing order"""

    def __init__(self):
        super(ChangeOrderCN, self).__init__()
        pass

    @classmethod
    def cn_pack_req(cls, cookie, envtype, localid, orderid, price, qty):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5005",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            "LocalID": localid,
                            "OrderID": orderid,
                            "Price": price_to_str_int1000(price),
                            "Qty": qty,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class AccInfoQueryCN(AccInfoQuery):
    """Class for querying information of account"""

    def __init__(self):
        super(AccInfoQueryCN, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, envtype):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5007",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class OrderListQueryCN(OrderListQuery):
    """Class for querying list queue"""

    def __init__(self):
        super(OrderListQueryCN, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, orderid, statusfilter, strcode, start, end, envtype):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5008",
               "Version": "1",
               "ReqParam": {"Cookie": str(cookie),
                            "EnvType": str(envtype),
                            "OrderID": str(orderid),
                            "StatusFilterStr": str(statusfilter),
                            "StockCode": strcode,
                            "start_time": start,
                            "end_time": end,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class PositionListQueryCN(PositionListQuery):
    """Class for querying position list"""

    def __init__(self):
        super(PositionListQueryCN, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, strcode, stocktype, pl_ratio_min, pl_ratio_max, envtype):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1 or (stocktype != '' and stocktype not in SEC_TYPE_MAP):
            error_str = ERROR_STR_PREFIX + "parameter envtype or stocktype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5009",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "StockCode": strcode,
                            "StockType": str(SEC_TYPE_MAP[stocktype]) if not stocktype == '' else '',
                            "PLRatioMin": price_to_str_int1000(pl_ratio_min),
                            "PLRatioMax": price_to_str_int1000(pl_ratio_max),
                            "EnvType": envtype,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class DealListQueryCN(DealListQuery):
    """Class for """

    def __init__(self):
        super(DealListQuery, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, envtype):
        """Convert from user request for trading days to PLS request"""
        if int(envtype) < 0 or int(envtype) > 1:
            error_str = ERROR_STR_PREFIX + "parameter envtype is wrong"
            return RET_ERROR, error_str, None

        req = {"Protocol": "5010",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_rsp(rsp_str)


class TradePushQueryCN(TradePushQuery):
    """ Query Trade push info"""

    def __init__(self):
        super(TradePushQuery, self).__init__()

    @classmethod
    def cn_pack_subscribe_req(cls, cookie, envtype, orderid_list, order_deal_push, push_at_once):
        """Pack the pushed response"""
        str_id = u''
        for orderid in orderid_list:
            if len(str_id) > 0:
                str_id += u','
            str_id += str(orderid)

        req = {"Protocol": "5100",
               "Version": "1",
               "ReqParam": {"Cookie": cookie,
                            "EnvType": envtype,
                            "OrderID": str_id,
                            "SubOrder": order_deal_push,
                            "SubDeal": order_deal_push,
                            "FirstPush": push_at_once,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_order_push_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_order_push_rsp(rsp_str)

    @classmethod
    def cn_unpack_deal_push_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        return cls.hk_unpack_deal_push_rsp(rsp_str)


class HistoryOrderListQueryCN(HistoryOrderListQuery):
    """Class for querying Histroy Order"""

    def __init__(self):
        super(HistoryOrderListQuery, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, statusfilter, strcode, start, end, envtype):
        """Convert from user request for trading days to PLS request"""
        req = {"Protocol": "5011",
               "Version": "1",
               "ReqParam": {"Cookie": str(cookie),
                            "EnvType": str(envtype),
                            "StatusFilterStr": str(statusfilter),
                            "StockCode": strcode,
                            "start_date": start,
                            "end_date": end,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response
        !!!这里有一点不一样在于，返回参数中，订单数组名为CNOrderArr!!!
        """
        ret, msg, rsp = extract_pls_rsp(rsp_str)
        if ret != RET_OK:
            return RET_ERROR, msg, None

        rsp_data = rsp['RetData']

        if 'EnvType' not in rsp_data:
            error_str = ERROR_STR_PREFIX + "cannot find EnvType in client rsp. Response: %s" % rsp_str
            return RET_ERROR, error_str, None

        if "CNOrderArr" not in rsp_data:
            error_str = ERROR_STR_PREFIX + "cannot find CNOrderArr in client rsp. Response: %s" % rsp_str
            return RET_ERROR, error_str, None

        raw_order_list = rsp_data["CNOrderArr"]
        if raw_order_list is None or len(raw_order_list) == 0:
            return RET_OK, "", []

        order_list = [{"code": merge_stock_str(1, order['StockCode']),
                       "stock_name": order["StockName"],
                       "dealt_qty": order['DealtQty'],
                       "qty": order['Qty'],
                       "orderid": order['OrderID'],
                       "order_type": order['OrderType'],
                       "order_side": order['OrderSide'],
                       "price": int1000_price_to_float(order['Price']),
                       "status": order['Status'],
                       "submited_time": order['SubmitedTime'],
                       "updated_time": order['UpdatedTime']
                       }
                      for order in raw_order_list]
        return RET_OK, "", order_list


class HistoryDealListQueryCN(HistoryDealListQuery):
    """Class for History DealList
    ！！！A股接口中CNDealArr字段和原来不一样。
    """

    def __init__(self):
        super(HistoryDealListQuery, self).__init__()

    @classmethod
    def cn_pack_req(cls, cookie, strcode, start, end, envtype):
        """Convert from user request for trading days to PLS request"""
        req = {"Protocol": "5012",
               "Version": "1",
               "ReqParam": {"Cookie": str(cookie),
                            "EnvType": str(envtype),
                            "StockCode": strcode,
                            "start_date": start,
                            "end_date": end,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def cn_unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        ret, msg, rsp = extract_pls_rsp(rsp_str)
        if ret != RET_OK:
            return RET_ERROR, msg, None

        rsp_data = rsp['RetData']

        if 'Cookie' not in rsp_data or 'EnvType' not in rsp_data:
            return RET_ERROR, msg, None

        if "CNDealArr" not in rsp_data:
            error_str = ERROR_STR_PREFIX + "cannot find CNDealArr in client rsp. Response: %s" % rsp_str
            return RET_ERROR, error_str, None

        raw_deal_list = rsp_data["CNDealArr"]
        if raw_deal_list is None or len(raw_deal_list) == 0:
            return RET_OK, "", []

        deal_list = [{"code": merge_stock_str(1, deal['StockCode']),
                      "stock_name": deal["StockName"],
                      "dealid": deal['DealID'],
                      "orderid": deal['OrderID'],
                      "qty": deal['Qty'],
                      "price": int1000_price_to_float(deal['Price']),
                      "time": deal['Time'],
                      "order_side": deal['OrderSide'],
                      "contra_broker_id": int(deal['ContraBrokerID']),
                      "contra_broker_name": deal['ContraBrokerName'],
                      }
                     for deal in raw_deal_list]
        return RET_OK, "", deal_list
