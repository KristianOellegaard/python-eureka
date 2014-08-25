import json
import random
from urllib2 import URLError
from urlparse import urljoin
from eureka import requests
import ec2metadata
import logging
import dns.resolver
from eureka.requests import EurekaHTTPException


logger = logging.getLogger('eureka.client')


class EurekaClientException(Exception):
    pass


class EurekaRegistrationFailedException(EurekaClientException):
    pass


class EurekaUpdateFailedException(EurekaClientException):
    pass


class EurekaHeartbeatFailedException(EurekaClientException):
    pass


class EurekaClient(object):
    def __init__(self, app_name, eureka_url=None, eureka_domain_name=None, host_name=None, data_center="Amazon",
                 vip_address=None, secure_vip_address=None, port=None, secure_port=None, use_dns=True, region=None,
                 prefer_same_zone=True, context="eureka/v2", eureka_port=None):
        super(EurekaClient, self).__init__()
        self.app_name = app_name
        self.eureka_url = eureka_url
        self.data_center = data_center
        if not host_name and data_center == "Amazon":
            self.host_name = ec2metadata.get("public-hostname")
        else:
            self.host_name = host_name
        # Virtual host name by which the clients identifies this service
        self.vip_address = vip_address
        self.secure_vip_address = secure_vip_address
        self.port = port
        self.secure_port = secure_port
        self.use_dns = use_dns
        # Region where eureka is deployed - For AWS specify one of the AWS regions, for other datacenters specify a
        # arbitrary string indicating the region.
        self.region = region
        # Prefer a eureka server in same zone or not
        self.prefer_same_zone = prefer_same_zone
        # Domain name, if using DNS
        self.eureka_domain_name = eureka_domain_name
        # Relative URL to eureka
        self.context = context
        self.eureka_port = eureka_port
        self.eureka_urls = self.get_eureka_urls()

    def _get_txt_records_from_dns(self, domain):
        records = dns.resolver.query(domain, 'TXT')
        for record in records:
            for string in record.strings:
                yield string

    def _get_zone_urls_from_dns(self, domain):
        for zone in self._get_txt_records_from_dns(domain):
            yield zone

    def get_zones_from_dns(self):
        return {
            zone_url.split(".")[0]: list(self._get_zone_urls_from_dns("txt.%s" % zone_url)) for zone_url in list(
                self._get_zone_urls_from_dns('txt.%s.%s' % (self.region, self.eureka_domain_name))
            )
        }

    def get_eureka_urls(self):
        if self.eureka_url:
            return [self.eureka_url]
        elif self.use_dns:
            zone_dns_map = self.get_zones_from_dns()
            zones = zone_dns_map.keys()
            assert len(zones) > 0, "No availability zones found for, please add them explicitly"
            if self.prefer_same_zone:
                if self.get_instance_zone() in zones:
                    zones = [zones.pop(zones.index(self.get_instance_zone()))] + zones  # Add our zone as the first element
                else:
                    logger.warn("No match for the zone %s in the list of available zones %s" % (
                        self.get_instance_zone(), zones)
                    )
            service_urls = []
            for zone in zones:
                eureka_instances = zone_dns_map[zone]
                random.shuffle(eureka_instances)  # Shuffle order for load balancing
                for eureka_instance in eureka_instances:
                    server_uri = "http://%s" % eureka_instance
                    if self.eureka_port != None:
                      server_uri += ":%s" % self.eureka_port
                    eureka_instance_url = urljoin(server_uri, self.context, "/")
                    if not eureka_instance_url.endswith("/"):
                        eureka_instance_url = "%s/" % eureka_instance_url
                    service_urls.append(eureka_instance_url)
            primary_server = service_urls.pop(0)
            random.shuffle(service_urls)
            service_urls.insert(0, primary_server)
            logger.info("This client will talk to the following serviceUrls in order: %s" % service_urls)
            return service_urls

    def get_instance_zone(self):
        if self.data_center == "Amazon":
            return ec2metadata.get('availability-zone')
        else:
            raise NotImplementedError("%s does not implement DNS lookups" % self.data_center)

    def register(self, initial_status="STARTING"):
        data_center_info = {
            'name': self.data_center
        }
        if self.data_center == "Amazon":
            data_center_info['metadata'] = {
                'ami-launch-index': ec2metadata.get('ami-launch-index'),
                'local-hostname': ec2metadata.get('local-hostname'),
                'availability-zone': ec2metadata.get('availability-zone'),
                'instance-id': ec2metadata.get('instance-id'),
                'public-ipv4': ec2metadata.get('public-ipv4'),
                'public-hostname': ec2metadata.get('public-hostname'),
                'ami-manifest-path': ec2metadata.get('ami-manifest-path'),
                'local-ipv4': ec2metadata.get('local-ipv4'),
                'ami-id': ec2metadata.get('ami-id'),
                'instance-type': ec2metadata.get('instance-type'),
            }
        instance_data = {
            'instance': {
                'hostName': self.host_name,
                'app': self.app_name,
                'vipAddr': self.vip_address or '',
                'secureVipAddr': self.secure_vip_address or '',
                'status': initial_status,
                'port': self.port,
                'securePort': self.secure_port,
                'dataCenterInfo': data_center_info
            }
        }
        success = False
        for eureka_url in self.eureka_urls:
            try:
                r = requests.post(urljoin(eureka_url, "apps/%s" % self.app_name), json.dumps(instance_data),
                                  headers={'Content-Type': 'application/json'})
                r.raise_for_status()
                success = True
                break
            except (EurekaHTTPException, URLError) as e:
                pass
        if not success:
            raise EurekaRegistrationFailedException("Did not receive correct reply from any instances")

    def update_status(self, new_status):
        success = False
        for eureka_url in self.eureka_urls:
            try:
                r = requests.put(urljoin(eureka_url, "apps/%s/%s/status?value=%s" % (
                    self.app_name,
                    self.host_name,
                    new_status
                )))
                r.raise_for_status()
                success = True
                break
            except (EurekaHTTPException, URLError) as e:
                pass
        if not success:
            raise EurekaUpdateFailedException("Did not receive correct reply from any instances")

    def heartbeat(self):
        instance_id = self.host_name
        if self.data_center == "Amazon":
            instance_id = ec2metadata.get('instance-id')
        success = False
        for eureka_url in self.eureka_urls:
            try:
                r = requests.put(urljoin(eureka_url, "apps/%s/%s" % (self.app_name, instance_id)))
                r.raise_for_status()
                success = True
                break
            except (EurekaHTTPException, URLError) as e:
                pass
        if not success:
            raise EurekaHeartbeatFailedException("Did not receive correct reply from any instances")





    def get_app_instances(self, app_id):
        for eureka_url in self.eureka_urls:
            try:
                r = requests.get(urljoin(eureka_url, "apps/%s" % (
                    app_id
                )), headers={ 'accept':'application/json'})
                r.raise_for_status()
                return json.loads(r.content)
            except (EurekaHTTPException, URLError) as e:
                pass
        if not success:
            raise EurekaUpdateFailedException("Did not receive correct reply from any instances")
