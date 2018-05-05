# -*- coding: utf-8 -*-
"""

"""

from datetime import timedelta
from futuquant.utils import *
from futuquant.quote_query import *


class CNGlobalStateQuery:
    """
    单独设置，请求a股接口交易程序的状态，采用和FUTU API一样的数据结构。
    但是Protocol改为1040，只需要对返回结构中的Trade_Logined字段进行判断。
    可以结合FUTU原来的GlobalStateQuery一起检查。
    """

    def __init__(self):
        pass

    @classmethod
    def pack_req(cls, state_type=0):
        """
        Convert from user request for trading days to PLS request
        :param state_type: for reserved, no use now !
        :return:  json string for request

        req_str: '{"Protocol":"1040","ReqParam":{"StateType":"0"},"Version":"1"}'
        """
        '''Parameter check'''

        # pack to json
        req = {"Protocol": "1029",
               "Version": "1",
               "ReqParam": {"StateType": state_type,
                            }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def unpack_rsp(cls, rsp_str):
        """
        Convert from PLS response to user response

        Example:

        rsp_str : '{"ErrCode":"0","ErrDesc":"","Protocol":"1029","RetData":{"Market_HK":"5",
        "Market_HKFuture":"15","Market_SH":"6","Market_SZ":"6","Market_US":"11","Quote_Logined":"1","Trade_Logined":"1"
        },"Version":"1"}\r\n\r\n'

         ret,msg,content = TradeDayQuery.unpack_rsp(rsp_str)

         ret : 0
         msg : ""
         content : {"Market_HK":"5",
                    "Market_HKFuture":"15",
                    "Market_SH":"6",
                    "Market_SZ":"6",
                    "Market_US":"11",
                    "Quote_Logined":"1",
                    "Trade_Logined":"1"
                    "TimeStamp":"1508250058"
                   }

        """
        # response check and unpack response json to objects
        ret, msg, rsp = extract_pls_rsp(rsp_str)
        if ret != RET_OK:
            return RET_ERROR, msg, None

        rsp_data = rsp['RetData']

        if 'Version' not in rsp_data:
            rsp_data['Version'] = ''

        return RET_OK, "", rsp_data


class HeartBeatPush:
    """
    HeartBeatPush  per 30 second
    """

    def __init__(self):
        pass

    @classmethod
    def unpack_rsp(cls, rsp_str):
        """
        Convert from PLS response to user response

        """
        # response check and unpack response json to objects
        ret, msg, rsp = extract_pls_rsp(rsp_str)
        if ret != RET_OK:
            return RET_ERROR, msg, None

        rsp_data = rsp['RetData']

        return RET_OK, '', int(rsp_data['TimeStamp'])


class MultiPointsHisKLine:
    """
    Query MultiPointsHisKLine
    """
    def __init__(self):
        pass

    @classmethod
    def pack_req(cls, codes, dates, fields, ktype, autype, max_num, no_data_mode):
        """Convert from user request for trading days to PLS request"""
        list_req_stock = []
        for stock_str in codes:
            ret, content = split_stock_str(stock_str)
            if ret == RET_ERROR:
                return RET_ERROR, content, None
            else:
                list_req_stock.append(content)

        for x in dates:
            ret, msg = check_date_str_format(x)
            if ret != RET_OK:
                return ret, msg, None

        if len(fields) == 0:
            fields = copy(KL_FIELD.ALL_REAL)
        str_field = ','.join(fields)
        list_req_field = KL_FIELD.get_field_list(str_field)
        if not list_req_field:
            return RET_ERROR, ERROR_STR_PREFIX + "field error", None

        if ktype not in KTYPE_MAP:
            error_str = ERROR_STR_PREFIX + "ktype is %s, which is not valid. (%s)" \
                                           % (ktype, ", ".join([x for x in KTYPE_MAP]))
            return RET_ERROR, error_str, None

        if autype not in AUTYPE_MAP:
            error_str = ERROR_STR_PREFIX + "autype is %s, which is not valid. (%s)" \
                                           % (autype, ", ".join([str(x) for x in AUTYPE_MAP]))
            return RET_ERROR, error_str, None

        req = {"Protocol": "1038",
               "Version": "1",
               "ReqParam": {
                   'Cookie': '10000',
                   'NoDataMode': str(no_data_mode),
                   'RehabType': str(AUTYPE_MAP[autype]),
                   'KLType': str(KTYPE_MAP[ktype]),
                   'MaxKLNum': str(max_num),
                   'StockArr': [
                        {'Market': str(market), 'StockCode': code}
                        for (market, code) in list_req_stock],
                   'TimePoints': str(','.join(dates)),
                   'NeedKLData': str(','.join(list_req_field))
                   }
               }
        req_str = json.dumps(req) + '\r\n'
        return RET_OK, "", req_str

    @classmethod
    def unpack_rsp(cls, rsp_str):
        """Convert from PLS response to user response"""
        ret, msg, rsp = extract_pls_rsp(rsp_str)
        if ret != RET_OK:
            return RET_ERROR, msg, None

        rsp_data = rsp['RetData']
        if "StockHistoryKLArr" not in rsp_data:
            error_str = ERROR_STR_PREFIX + "cannot find StockHistoryKLArr in client rsp. Response: %s" % rsp_str
            return RET_ERROR, error_str, None
        has_next = int(rsp_data['HasNext'])

        list_ret = []
        dict_data = {}
        arr_kline = rsp_data["StockHistoryKLArr"]
        for kline in arr_kline:
            stock_str = merge_stock_str(int(kline['Market']), kline['StockCode'])
            data_arr = kline['HistoryKLArr']
            for point_data in data_arr:
                dict_data['code'] = stock_str
                dict_data['time_point'] = point_data['TimePoint']
                dict_data['data_valid'] = int(point_data['DataValid'])
                if 'Time' in point_data:
                    dict_data['time_key'] = point_data['Time']
                if 'Open' in point_data:
                    dict_data['open'] = int10_9_price_to_float(point_data['Open'])
                if 'High' in point_data:
                    dict_data['high'] = int10_9_price_to_float(point_data['High'])
                if 'Low' in point_data:
                    dict_data['low'] = int10_9_price_to_float(point_data['Low'])
                if 'Close' in point_data:
                    dict_data['close'] = int10_9_price_to_float(point_data['Close'])
                if 'Volume' in point_data:
                    dict_data['volume'] = point_data['Volume']
                if 'Turnover' in point_data:
                    dict_data['turnover'] = int1000_price_to_float(point_data['Turnover'])
                if 'PERatio' in point_data:
                    dict_data['pe_ratio'] = int1000_price_to_float(point_data['PERatio'])
                if 'TurnoverRate' in point_data:
                    dict_data['turnover_rate'] = int1000_price_to_float(point_data['TurnoverRate'])
                if 'ChangeRate' in point_data:
                    dict_data['change_rate'] = int1000_price_to_float(point_data['ChangeRate'])

                list_ret.append(dict_data.copy())

        return RET_OK, "", (list_ret, has_next)
