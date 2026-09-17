"""Explicit expectations are independent of runtime contract observations."""
from copy import deepcopy
import unittest

from collect import build_verdict_inputs
from matrix import Cell


class PinnedCaptureTests(unittest.TestCase):
    def test_pins_preserve_observation_duration_and_receipt_window_separately(self):
        cell = Cell('channel', 'idle', 'resumed', 'text', 'single')
        original = build_verdict_inputs(cell, {})
        scenario, contract = deepcopy(original[:2])
        scenario['observation_duration_ms'] = 80000
        contract['id'] = 'explicitly-selected-contract'
        inputs = build_verdict_inputs(cell, {'contract': {'axes': {'receipt_type': 'none'}}},
                                      pinned_scenario=scenario, pinned_contract=contract)
        self.assertEqual(inputs[:2], (scenario, contract))
        self.assertEqual(inputs[0]['observation_duration_ms'], 80000)
        self.assertEqual(inputs[1]['timing']['live']['limit_ms'], 15000)
        self.assertEqual(inputs[1]['axes']['receipt_type'], 'transcript')
        scenario['observation_duration_ms'] = 1
        contract['axes']['receipt_type'] = 'none'
        self.assertEqual(inputs[0]['observation_duration_ms'], 80000)
        self.assertEqual(inputs[1]['axes']['receipt_type'], 'transcript')
        self.assertEqual(build_verdict_inputs(cell, {}), original)

    def test_pins_are_required_as_a_pair(self):
        cell = Cell('fetch', 'idle', 'resumed', 'text', 'single')
        scenario, contract, _ = build_verdict_inputs(cell, {})
        for pins in ({'pinned_scenario': scenario}, {'pinned_contract': contract}):
            with self.assertRaisesRegex(ValueError, 'pinned together'):
                build_verdict_inputs(cell, {}, **pins)


if __name__ == '__main__':
    unittest.main()
