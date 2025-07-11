from json import loads, dumps
from kafka import KafkaConsumer
from django.conf import settings
import logging
import sys

from models import Order
from consumer_olm.constants import OrderDeliveryType

_GROUP_ID = ''


def get_logger():
    logging.basicConfig(
        format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d:%H:%M:%S',
        level=logging.INFO)
    _logger = logging.getLogger(__name__)
    return _logger


logger = get_logger()
def update_order(reference_id, lat, lng, accuracy_level):
    try:
        order_obj = Order.objects.using('orders_db').get(cp_awb=reference_id).first()

        if order_obj:
            if order_obj.delivery_type == OrderDeliveryType.FORWARD:
                order_obj.drop_info_latitude = lat
                order_obj.drop_info_longitude = lng
            else:
                order_obj.pickup_info_latitude = lat
                order_obj.pickup_info_longitude = lng

            order_obj.lat_long_accuracy_level = accuracy_level
            order_obj.save(using='orders_db')
        logger.info(
                f"LM_CONSUMER_UPDATION_LOGS : - {reference_id} - " )
    except Exception as e:
        logger.error(
            f"LM_CONSUMER_UPDATION_DATABASE: - {reference_id} - {e} ")

def consume():
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER,
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False
        # group_id=_GROUP_ID
    )

    for message in consumer:
        data = message.value
        try:
            reference_id = data.get('reference_id')
            lat = data.get('geocoded_location', {}).get('lat')
            lng = data.get('geocoded_location', {}).get('lng')
            accuracy_level = data.get('accuracy_level')

            if reference_id and lat and lng and accuracy_level is not None:
                update_order(reference_id, lat, lng, accuracy_level)
            else:
                logger.error(
                    f"LM_CONSUMER_UPDATION_ERROR_LOGS : - {reference_id} - data- {data} ")
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            logger.error(
                f"LM_CONSUMER_ERROR_LOG : failure offset - <{message.offset}> - data - <{message.value}> - partition - <{message.partition}> -line- <{exc_tb.tb_lineno}> - type- {exc_type} - exception - {exc_value}")
        finally:
            logger.info(
                f'LM_CONSUMER_LOGS : - commiting offset - <{message.offset}> - data - <{message.value}> - partition - <{message.partition}>')
            consumer.commit()

consume()