# -*- coding: utf-8 -*-
"""
    Constant collection
"""
from futuquant.constant import *

# noinspection PyPep8Naming
class TRADE_CN(TRADE):

    # A股暂时只支持真实环境
    @staticmethod
    def check_envtype_cn(envtype):
        return str(envtype) == u'0'

