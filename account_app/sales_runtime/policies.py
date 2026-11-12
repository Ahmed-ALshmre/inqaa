"""Owner-approved defaults; explicit store policy values take precedence."""
from ..checkout import PROVINCES

# Confirmed by the owner for all five stores on 2026-11-10.
APPROVED_DELIVERY_TIME='من 24 ساعة إلى يومين'


def view(store_id,confirmation,fee,custom=None):
    policy=dict(inspection_policy=confirmation.split('. ',1)[-1],payment='الدفع عند الاستلام بعد الفحص',
                delivery_time=APPROVED_DELIVERY_TIME,delivery_fee={p:fee(store_id,p) for p in PROVINCES})
    for field,value in (custom or {}).items():
        if value not in (None,'') and field in {'inspection_policy','payment','delivery_time','delivery_fee','shop_address'}:
            policy[field]=value
    return policy
