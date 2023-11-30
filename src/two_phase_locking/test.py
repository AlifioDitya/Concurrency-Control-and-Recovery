import unittest

import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)

from two_phase_locking import TwoPhaseLocking, parse_input, Operation, LockType, LockManager

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

        t.locks_manager.lock_data(1, "A", LockType.X)

        self.assertTrue(t.locks_manager.is_locked_by(1))
        self.assertTrue(t.locks_manager.is_locked("A"))

    def test_unlock(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)
        t.locks_manager.lock_data(1, "A", LockType.X)
        t.locks_manager.unlock(1)

        self.assertFalse(t.locks_manager.is_locked_by(1))
        self.assertFalse(t.locks_manager.is_locked("A"))


    def test_queue(self):
        schedule ="R1(A); W1(A); C1"
        list_schedule = parse_input(schedule)
        t = TwoPhaseLocking(list_schedule)
        t.add_queue(Operation.READ, 1, "A")

        self.assertTrue(t.is_waiting(1))

        t.remove_queue(1)

        self.assertFalse(t.is_waiting(1))

        t.queue_to_schedule()

        self.assertEqual(t.waiting_queue, [])

    def test_result_simple(self):
        sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
        sample_input_2 = "R1(X); R2(Y); R1(Y); W2(Y); W1(X); C1; C2;"
        sample_input_3 = "R1(X); R2(Y); R1(Y); R2(X); C1; C2;"            # no deadlock because shared unless not upgrade
        sample_input_4 = "R1(X) ;W2(Y) ;W2(X); W3(Y) ;W1(Y); C1; C2; C3;" # deadlock

        s1 = parse_input(sample_input_1)
        s2 = parse_input(sample_input_2)
        s3 = parse_input(sample_input_3)
        s4 = parse_input(sample_input_4)

        t1 = TwoPhaseLocking(s1, LockManager())
        t1.run(upgrade=False)
        t2 = TwoPhaseLocking(s2, LockManager())
        t2.run(upgrade=False)


        t1_result = ['XL1(X)', 'R1(X)', 'XL3(Y)', 'W3(Y)', 'W1(X)', 'C1', 
                    'UL1(X)', 'XL2(X)', 'W2(X)', 'C3',
                    'UL3(Y)', 'XL2(Y)', 'W2(Y)', 'C2', 'UL2(X)', 'UL2(Y)']
        t2_result = ['XL1(X)', 'R1(X)', 'XL2(Y)', 'R2(Y)', 'W2(Y)', 'C2', 
                    'UL2(Y)', 'XL1(Y)', 'R1(Y)', 'W1(X)', 'C1', 'UL1(X)', 'UL1(Y)']
        
        self.assertEqual(t1.result, t1_result)
        self.assertEqual(t2.result, t2_result)
        # Raise Exception("Deadlock detected")
        t3 = TwoPhaseLocking(s3, LockManager())
        self.assertRaises(Exception, t3.run, upgrade=False)
        t4 = TwoPhaseLocking(s4, LockManager())
        self.assertRaises(Exception, t4.run, upgrade=False)

    def test_result_upgrade(self):
        sample_input_1 = "R1(X); W2(X); W2(Y); W3(Y); W1(X); C1; C2; C3;"
        sample_input_2 = "R1(X); R2(Y); R1(Y); W2(Y); W1(X); C1; C2;"
        sample_input_3 = "R1(X); R2(Y); R1(Y); R2(X); C1; C2;"            # no deadlock because shared unless not upgrade
        sample_input_4 = "R1(X) ;W2(Y) ;W2(X); W3(Y) ;W1(Y); C1; C2; C3;" # deadlock
            
        s1 = parse_input(sample_input_1)
        s2 = parse_input(sample_input_2)
        s3 = parse_input(sample_input_3)
        s4 = parse_input(sample_input_4)

        t1 = TwoPhaseLocking(s1, LockManager())
        t1.run(upgrade=True)
        t2 = TwoPhaseLocking(s2, LockManager())
        t2.run(upgrade=True)
        t3 = TwoPhaseLocking(s3, LockManager())
        t3.run(upgrade=True)

        t1_result = ['SL1(X)', 'R1(X)', 'XL3(Y)', 'W3(Y)', 'XU1(X)' , 'W1(X)', 'C1', 
            'UL1(X)', 'XL2(X)', 'W2(X)', 'C3',
            'UL3(Y)', 'XL2(Y)', 'W2(Y)', 'C2', 'UL2(X)', 'UL2(Y)']
        
        t2_result = ['SL1(X)', 'R1(X)', 'SL2(Y)', 'R2(Y)', 'SL1(Y)', 'R1(Y)', 'XU1(X)', 
            'W1(X)', 'C1', 'UL1(X)', 'UL1(Y)', 'XL2(Y)', 'W2(Y)', 'C2', 'UL2(Y)']
        
        t3_result = ['SL1(X)', 'R1(X)', 'SL2(Y)', 'R2(Y)', 'SL1(Y)', 'R1(Y)', 'SL2(X)', 'R2(X)',
                'C1', 'UL1(X)', 'UL1(Y)', 'C2', 'UL2(Y)', 'UL2(X)']

        self.assertEqual(t1.result, t1_result)
        self.assertEqual(t2.result, t2_result)
        self.assertEqual(t3.result, t3_result)
        # Raise Exception("Deadlock detected")
        t4 = TwoPhaseLocking(s4, LockManager())
        self.assertRaises(Exception, t4.run, upgrade=True)


if __name__ == '__main__':
    unittest.main()
