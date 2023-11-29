import unittest

import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)

from two_phase_locking import TwoPhaseLocking, parse_input, Operation, LockType

class TestTwoPhaseLocking(unittest.TestCase):


    def test_parse_schedule(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)
        
        expected = [(Operation.READ, 1, "A"), (Operation.WRITE, 1, "A"), (Operation.COMMIT, 1, "")]

        self.assertEqual(t.parsed_schedule, expected)

    def test_lock(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)

        self.assertTrue(t.lock_data_item(1, "A", LockType.X))

        self.assertTrue(t.is_locked_by(1))
        self.assertTrue(t.is_locked("A"))

    def test_unlock(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)
        t.lock_data_item(1, "A", LockType.X)
        t.unlock(1)

        self.assertFalse(t.is_locked_by(1))
        self.assertFalse(t.is_locked("A"))


    def test_queue(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)
        t.add_to_queue(Operation.READ, 1, "A")

        self.assertTrue(t.is_waiting(1))

        t.remove_from_queue(1)

        self.assertFalse(t.is_waiting(1))

        t.queue_to_schedule()

        self.assertEqual(t.waiting_queue, [])


if __name__ == '__main__':
    unittest.main()
