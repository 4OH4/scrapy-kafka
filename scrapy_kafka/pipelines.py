# -*- coding: utf-8 -*-
from scrapy.utils.serialize import ScrapyJSONEncoder

from kafka import KafkaProducer


class KafkaPipeline(object):

    """
    Publishes a serialized item into a Kafka topic

    :param producer: The Kafka producer
    :type producer: kafka.producer.Producer

    :param topic: The Kafka topic being used
    :type topic: str or unicode

    """

    def __init__(self, producer, topic):
        """
        :type producer: kafka.producer.Producer
        :type topic: str or unicode
        """
        self.producer = producer
        self.topic = topic

        self.encoder = ScrapyJSONEncoder()

    def process_item(self, item, spider):
        """
        Overriden method to process the item

        :param item: Item being passed
        :type item: scrapy.item.Item

        :param spider: The current spider being used
        :type spider: scrapy.spider.Spider
        """
        # put spider name in item
        item = dict(item)
        item['spider'] = spider.name
        msg = self.encoder.encode(item)
        self.producer.send(self.topic, value=bytes(msg, encoding='utf-8'))

    @classmethod
    def from_settings(cls, settings):
        """
        :param settings: the current Scrapy settings
        :type settings: scrapy.settings.Settings

        :rtype: A :class:`~KafkaPipeline` instance
        """
        k_hosts = settings.get('SCRAPY_KAFKA_HOSTS', ['localhost:9092'])
        topic = settings.get('SCRAPY_KAFKA_ITEM_PIPELINE_TOPIC', 'scrapy_kafka_item')
        conn = KafkaProducer(bootstrap_servers=k_hosts)
        return cls(conn, topic)
