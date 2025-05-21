from enum import Enum
class OrderDeliveryType(str, Enum):
    FORWARD = 'FORWARD'
    REVERSE = 'RETURN'