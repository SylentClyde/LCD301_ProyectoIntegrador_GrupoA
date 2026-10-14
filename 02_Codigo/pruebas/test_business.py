"""Pruebas de invariantes; importes enteros, duplicados y referencias fuera de orden."""
import copy
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'procesamiento'))
from database import decide


class BusinessRules(unittest.TestCase):
    def setUp(self):
        self.item=dict(order_line_id='L1',product_id='PRD001',quantity=3,
            unit_price_cents=3000,discount_cents=500,refund_cents=None,
            product_name='Producto',category='hogar',catalog_version=1,net_cents=8500)
        self.created=dict(event_id='C1',event_type='created',order_id='O1',customer_id='U1',
            event_time='2026-04-30T23:59:00Z',channel='web',currency='BOB',total_cents=8500,
            original_payment_id=None,payload_hash='created',items=[dict(self.item)],_lines=[dict(self.item)])
        self.paid=copy.deepcopy(self.created)
        self.paid.update(event_id='P1',event_type='paid',event_time='2026-05-01T00:01:00Z',payload_hash='paid')
        self.registry={'C1':self.created,'P1':self.paid}
        self.orders={'O1':[self.created,self.paid]}
        self.lines={('P1','L1'):self.item}
        self.refund=copy.deepcopy(self.paid)
        self.refund.update(event_id='R1',event_type='refunded',event_time='2026-05-02T00:00:00Z',
                           original_payment_id='P1',total_cents=2833,payload_hash='refund')
        self.refund['items'][0].update(quantity=1,refund_cents=2833)

    def decision(self,event,returned=None):
        return decide(event,self.registry,self.orders,self.lines,returned or {})[0:2]

    def test_discounted_partial_refund_integer_rounding(self):
        self.assertEqual(self.decision(self.refund)[0],'accepted')
        self.refund['items'][0]['refund_cents']=2834
        self.assertEqual(self.decision(self.refund)[1],'refund_amount_exceeds_proportional_paid_amount')

    def test_cumulative_quantity_prevents_double_return(self):
        self.assertEqual(self.decision(self.refund,{('P1','L1'):(3,8500)})[1],
                         'refund_quantity_exceeds_paid_quantity')

    def test_cumulative_amount_prevents_overpayment(self):
        self.refund['items'][0]['refund_cents']=3000
        self.assertEqual(self.decision(self.refund,{('P1','L1'):(1,2833)})[1],
                         'refund_amount_exceeds_proportional_paid_amount')

    def test_two_rounded_partials_allow_final_remaining_cent(self):
        self.refund['items'][0]['refund_cents']=2834
        self.assertEqual(self.decision(self.refund,{('P1','L1'):(2,5666)})[0],'accepted')

    def test_same_id_identical_is_idempotent(self):
        self.assertEqual(self.decision(self.paid)[0],'duplicate')

    def test_same_id_changed_is_rejected(self):
        changed=dict(self.paid,payload_hash='changed')
        self.assertEqual(self.decision(changed)[0],'rejected')

    def test_second_payment_cannot_recognize_revenue(self):
        self.assertEqual(self.decision(dict(self.paid,event_id='P2'))[1],
                         'payment_requires_unpaid_uncancelled_order')

    def test_refund_wrong_customer(self):
        self.assertEqual(self.decision(dict(self.refund,customer_id='OTHER'))[1],
                         'order_customer_channel_currency_mismatch')

    def test_refund_unknown_payment_is_pending(self):
        self.assertEqual(self.decision(dict(self.refund,original_payment_id='NOT_YET'))[0],'pending')

    def test_payment_without_created_is_pending(self):
        self.assertEqual(decide(self.paid,{}, {},{}, {})[0],'pending')

    def test_refund_must_follow_payment_timestamp(self):
        self.assertEqual(self.decision(dict(self.refund,event_time='2026-05-01T00:00:00Z'))[1],
                         'refund_before_payment')

    def test_cancel_after_paid_rejected(self):
        event=dict(self.refund,event_id='X1',event_type='cancelled',total_cents=0,items=[])
        self.assertEqual(self.decision(event)[1],'cancel_requires_unpaid_uncancelled_order')


if __name__=='__main__':
    unittest.main(verbosity=2)
