# -*- coding: utf-8 -*-
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider
from kafka import KafkaConsumer


class KafkaSpiderMixin(object):

    """
    Mixin class to implement reading urls from a kafka queue.

    :type kafka_topic: str
    """
    topic = None

    def process_kafka_message(self, message):
        """"
        Tell this spider how to extract urls from a kafka message

        :param message: A Kafka message object
        :type message: kafka.common.OffsetAndMessage
        :rtype: str or None
        """
        if not message:
            return None

        return message.value.decode()

    def setup_kafka(self, settings, **configs):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.

        :param settings: The current Scrapy settings being used
        :type settings: scrapy.settings.Settings
        """
        if not hasattr(self, 'topic') or not self.topic:
            self.topic = '%s-starturls' % self.name

        hosts = settings.get('SCRAPY_KAFKA_HOSTS', ['localhost:9092'])
        consumer_group = settings.get('SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP', 'scrapy-kafka')

        # wait at most 1sec for more messages. Otherwise continue
        self.consumer = KafkaConsumer(self.topic, group_id=consumer_group,
                                      bootstrap_servers=hosts, consumer_timeout_ms=100,
                                      request_timeout_ms=190000, session_timeout_ms=180000, **configs)

        # idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from kafka topic
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)
        self.log("Reading messages from kafka topic '%s'" % self.topic)

    def next_request(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        message = next(self.consumer)
        url = self.process_kafka_message(message)
        if not url:
            return None
        return self.make_requests_from_url(url)

    def schedule_next_request(self):
        """Schedules a request if available"""
        try:
            req = self.next_request()
            if req:
                self.crawler.engine.crawl(req, spider=self)
        except StopIteration:
            self.log("No messages in kafka")

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """Avoids waiting for the spider to  idle before scheduling the next request"""
        self.schedule_next_request()


class ListeningKafkaSpider(KafkaSpiderMixin, Spider):

    """
    Spider that reads urls from a kafka topic when idle.

    This spider will exit only if stopped, otherwise it keeps
    listening to messages on the given topic

    Specify the topic to listen to by setting the spider's `kafka_topic`.

    Messages are assumed to be URLS, one by message. To do custom
    processing of kafka messages, override the spider's `process_kafka_message`
    method
    """

    def _set_crawler(self, crawler):
        """
        :type crawler: scrapy.crawler.Crawler
        """
        super(ListeningKafkaSpider, self)._set_crawler(crawler)
        self.setup_kafka(crawler.settings, **self.configs if hasattr(self, "configs") else {})
