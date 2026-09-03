from __future__ import annotations


class FencedExchangeFacade:
    """Fence immediately before/after external I/O and prioritize safety requests."""
    _PRIORITY={
        'emergency_market_sell':0,
        'place_protection':0,
        'protection_details_by_client_id':0,
        'query_order_by_client_id':1,
        'cancel_remainder':1,
        'submit_market_buy':2,
        'get_free_base_balance':2,
        'get_market_price':5,
    }
    def __init__(self,raw_exchange,lease,holder_id:str,epoch:int,rate_governor=None):
        self.__raw=raw_exchange;self._lease=lease;self._holder=holder_id;self._epoch=int(epoch);self._rate_governor=rate_governor
    def _call(self,name,*args,**kwargs):
        self._lease.assert_epoch('execution-core',self._holder,self._epoch)
        if self._rate_governor is not None:self._rate_governor.acquire(priority=self._PRIORITY.get(name,3))
        result=getattr(self.__raw,name)(*args,**kwargs)
        self._lease.assert_epoch('execution-core',self._holder,self._epoch)
        return result
    def get_market_price(self,*a,**k):return self._call('get_market_price',*a,**k)
    def submit_market_buy(self,*a,**k):return self._call('submit_market_buy',*a,**k)
    def query_order_by_client_id(self,*a,**k):return self._call('query_order_by_client_id',*a,**k)
    def cancel_remainder(self,*a,**k):return self._call('cancel_remainder',*a,**k)
    def get_free_base_balance(self,*a,**k):return self._call('get_free_base_balance',*a,**k)
    def place_protection(self,*a,**k):return self._call('place_protection',*a,**k)
    def protection_details_by_client_id(self,*a,**k):return self._call('protection_details_by_client_id',*a,**k)
    def emergency_market_sell(self,*a,**k):return self._call('emergency_market_sell',*a,**k)
