import csv
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import hpc.autoscale.hpclogging as log
from collections import namedtuple
from requests_cache import CachedSession

class azurecost:
    def __init__(self, config: dict):

        self.config = config
        self.retail_url = "https://prices.azure.com/api/retail/prices?api-version=2021-10-01-preview&meterRegion='primary'"
        if config.get('cluster_name'):
            self.clusters = config.get('cluster_name')
        else:
            raise ValueError("cluster_name must be present in config")
        cost_config = config.get('cost', {})
        if not cost_config or not cost_config.get('cache_root'):
            log.info("Defaulting cost cache dir to /tmp")
            self.cache_root = "/tmp"
        else:
            self.cache_root = cost_config.get("cache_root")
        self.dimensions = namedtuple("dimensions", "cost,usage,region,meterid,meter,metercat,metersubcat,resourcegroup,tags,currency")
        retail_name = f"{self.cache_root}/retail"
        self.retail_session = CachedSession(cache_name=retail_name,
                                            backend='filesystem',
                                            allowable_codes=(200,),
                                            allowable_methods=('GET'),
                                            expire_after=172800)

        #_az_logger = logging.getLogger('azure.identity')
        #_az_logger.setLevel(logging.ERROR)


        ## If we have ACM data available use azcost format else use retail format.
        ## for nodearray, combine usage data with either azcost or retail format.
        self.DEFAULT_AZCOST_FORMAT="sku_name,region,spot,meter,meterid,metercat,metersubcat,resourcegroup,rate,currency"
        self.RETAIL_FORMAT="sku_name,region,spot,meter,meterid,metercat,rate,currency"
        self.NODEARRAY_USAGE_FORMAT="nodearray,core_hours,cost"
        self.NODEARRAY_USAGE_HOURLY_FORMAT="start,end,nodearray,core_hours,cost"

        self.az_job_t = namedtuple('az_job_t', self.DEFAULT_AZCOST_FORMAT)
        self.az_job_retail_t = namedtuple('az_job_retail_t', self.RETAIL_FORMAT)
        self.az_array_t = namedtuple('az_array_t', (self.NODEARRAY_USAGE_FORMAT + ',' + self.DEFAULT_AZCOST_FORMAT))
        self.az_array_retail_t = namedtuple('az_array_retail_t', (self.NODEARRAY_USAGE_FORMAT + ',' + self.RETAIL_FORMAT))
        self.az_array_hourly_t = namedtuple('az_array_hourly_t', (self.NODEARRAY_USAGE_HOURLY_FORMAT + ',' + self.DEFAULT_AZCOST_FORMAT))
        self.az_array_hourly_retail_t = namedtuple('az_array_hourly_retail_t', (self.NODEARRAY_USAGE_HOURLY_FORMAT + ',' + self.RETAIL_FORMAT))

    def do_meter_lookup(self, sku_name, spot, region):
        """
        check cache storage if we have seen this meter's rate
        before.
        """
        return None

    def check_cost_avail(self, start=None, end=None):
        """
        For a given time period, check if we have
        cost data available. If time period is not
        specified, check if historical cost data
        is available to allow us to infer rates.
        """
        return False

    def get_job_format(self):

        if self.check_cost_avail():
            return self.az_job_t
        else:
            return self.az_job_retail_t

    def get_job(self, sku_name, region, spot):

        _fmt = self.get_job_format()

        if _fmt.__name__ == 'az_job_retail_t':
            data = self.get_retail_rate(sku_name, region, spot)
            az_fmt = _fmt(sku_name=sku_name, region=region,spot=spot,meter=data['meterName'],
                                meterid=data['meterId'],metercat=data['serviceName'],
                                rate=data['retailPrice'], currency=data['currencyCode'])
            return az_fmt
        else:
            pass

    def get_nodearray_format(self):

        if self.check_cost_avail():
            return self.az_array_t
        else:
            return self.az_array_retail_t

    def get_nodearray_hourly_format(self):

        if self.check_cost_avail():
            return self.az_array_hourly_t
        else:
            return self.az_array_hourly_retail_t

    def get_nodearray(self, fout, start, end):

        def _process_usage_with_retail(az_fmt_t, fout, usage: dict):

            writer = csv.writer(fout, delimiter=',')
            for e in usage['usage'][0]['breakdown']:
                if e['category'] != 'nodearray':
                    continue
                array_name = e['node']
                for a in e['details']:
                    sku_name = a['vm_size']
                    region = a['region']
                    hours = a['hours']
                    core_count = a['core_count']
                    spot = False
                    if a['priority'] == 'Spot':
                        spot = True
                    if self.do_meter_lookup(sku_name=sku_name,region=region,spot=spot):
                        pass
                    # use retail data
                    data = self.get_retail_rate(armskuname=sku_name, armregionname=region, spot=spot)
                    rate = data['retailPrice']
                    cost =  (hours/core_count) * rate
                    array_fmt = az_fmt_t(sku_name=sku_name, region=region,spot=spot,core_hours=hours, cost=cost,
                                        nodearray=array_name,meterid=data['meterId'],meter=data['meterName'],
                                        metercat=data['serviceName'], rate=data['retailPrice'], currency=data['currencyCode'])
                    row = []
                    for f in az_fmt_t._fields:
                        row.append(array_fmt._asdict()[f])
                    writer.writerow(row)

            return

        usage = self.get_usage(start, end, 'total')
        _fmt = self.get_nodearray_format()
        if _fmt.__name__ == 'az_array_retail_t':
            return _process_usage_with_retail(_fmt, fout, usage)

    def get_nodearray_hourly(self, fout, start, end):

        def _process_hourly_with_retail(az_fmt_t, fout, usage: dict):
            writer = csv.writer(fout, delimiter=',')
            for e in usage['usage']:
                start = e['start']
                end = e['end']
                for a in e['breakdown']:
                    if a['category'] != 'nodearray':
                        continue
                    array = a['node']
                    for d in a['details']:
                        vm_size = d['vm_size']
                        region = d['region']
                        core_count = d['core_count']
                        hours = d['hours']
                        spot = False
                        if d['priority'] == 'Spot':
                            spot = True
                        if self.do_meter_lookup(sku_name=vm_size,region=region,spot=spot):
                            pass
                        rate_data = self.get_retail_rate(armskuname=vm_size, armregionname=region, spot=spot)
                        rate = rate_data['retailPrice']
                        cost = (hours/core_count) * rate
                        array_fmt = az_fmt_t(start=start, end=end, nodearray=array, core_hours=hours, cost=cost,
                                             sku_name=vm_size,region=region,spot=spot,meterid=rate_data['meterId'],
                                             metercat=rate_data['serviceName'], rate=rate_data['retailPrice'],
                                             meter=rate_data['meterName'], currency=rate_data['currencyCode'])
                        row = []
                        for f in az_fmt_t._fields:
                            row.append(array_fmt._asdict()[f])
                        writer.writerow(row)

        usage = self.get_usage(start, end, 'hourly')
        _fmt = self.get_nodearray_hourly_format()
        if _fmt.__name__ == 'az_array_hourly_retail_t':
            return _process_hourly_with_retail(_fmt, fout, usage)

    def get_retail_rate(self, armskuname: str, armregionname: str, spot: bool):

        params = {}
        filters = f"armRegionName eq '{armregionname}' and armSkuName eq '{armskuname}' and serviceName eq 'Virtual Machines'"
        params['$filter'] = filters

        res = self.retail_session.get(self.retail_url, params=params)
        if res.status_code != 200:
            log.error(f"{res.json()}")
            raise res.raise_for_status()
        data = res.json()

        for e in data['Items']:
            if e['type'] != 'Consumption':
                continue

            if e['productName'].__contains__("Windows"):
                continue

            if e['meterName'].__contains__("Low Priority"):
                continue

            if e['meterName'].__contains__("Spot"):
                if spot:
                    return e
                continue
            return e

    def get_info_from_retail(self, meterId: str):

        sku = 'armSkuName'
        region = 'armRegionName'
        filters = f"meterId eq '{meterId}'"
        params = {}
        params['$filter'] = filters

        res = self.retail_session.get(self.retail_url, params=params)
        if res.status_code != 200:
            log.error(f"{res.json()}")
            raise res.raise_for_status()

        data = res.json()
        sku_list = []
        for e in data['Items']:
            if e.get(sku) and e.get(region):
                sku_list.append((e[sku],e[region]))
        return sku_list

    def get_usage(self, start: str, end: str, granularity: str):

        clustername = self.config['cluster_name']
        endpoint = f"{self.config['url']}/clusters/{clustername}/usage?expand=details"
        params = {}
        params['granularity'] = granularity
        params['timeframe'] = 'custom'
        params['from'] = start
        params['to'] = end
        uname = self.config['username']
        pw = self.config['password']

        urllib3.disable_warnings(InsecureRequestWarning)
        res = requests.get(url=endpoint, params=params, auth=(uname,pw), verify=False)
        if res.status_code != 200:
            log.error(res.reason)
            res.raise_for_status()

        return res.json()
