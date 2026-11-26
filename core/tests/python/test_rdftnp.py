"""Graph helpers keep probe decisions outside denominator-pair construction."""
import unittest
from unittest.mock import patch
import rdftnp

class Node:
    def __init__(self):self.definitions=[]
    def Define(self,name,expression):self.definitions.append((name,expression));return self

class TagProbeGraphTest(unittest.TestCase):
    @patch('rdftnp._declare')
    def test_pair_has_no_measured_decision_input(self,declare):
        node=Node();keys=['pt','eta','phi','mass','charge','denominator','tags','run','lumi','event']
        result=rdftnp.define_pairs(node,'z',{k:k for k in keys},seed=9)
        self.assertIs(result,node)
        self.assertIn('selectPair(pt,eta,phi,mass,charge,denominator,tags,run,lumi,event,',node.definitions[0][1])
        self.assertEqual(node.definitions[1],('z_valid','z_pair[0]>=0'))
        with self.assertRaises(ValueError):rdftnp.define_pairs(Node(),'z',{**{k:k for k in keys},'probe_pass':'pass'})
    @patch('rdftnp._declare')
    def test_reject_expressions_before_interpreter(self,declare):
        keys=['eta','phi','trigger_pt','trigger_eta','trigger_phi','trigger_id','trigger_bits','fired']
        mapping={k:k for k in keys};mapping['fired']='a || b'
        with self.assertRaises(ValueError):rdftnp.define_trigger_matches(Node(),'e',mapping,absolute_id=11,required_bits=2)
        declare.assert_not_called()
    @patch('rdftnp._declare')
    def test_trigger_all_bits_explicit_event_gate(self,declare):
        node=Node();keys=['eta','phi','trigger_pt','trigger_eta','trigger_phi','trigger_id','trigger_bits','fired']
        rdftnp.define_trigger_matches(node,'mu',{k:k for k in keys},absolute_id=13,required_bits=10)
        self.assertIn('TriggerMatchConfig{13,10U',node.definitions[0][1])

if __name__=='__main__':unittest.main()
