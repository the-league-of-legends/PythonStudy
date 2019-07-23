class TestResult(object):
    def __init__(self):
        self.result = []
        self.case_result = dict()
        self.case_state = -1
        self.case_log = []

    def test_start(self, name):
        self.case_state = 0
        self.case_result = dict()
        self.case_result['name'] = name
        self.case_log = []

    def test_failed(self, msg):
        self.case_result['status'] = False
        self.case_result['desc'] = msg
        self.test_stop()

    def test_success(self):
        self.case_result['status'] = True
        self.case_result['desc'] = ''
        self.test_stop()

    def test_no_expected(self):
        self.case_result['status'] = True
        self.case_result['desc'] = 'No expected test'
        self.test_stop()

    def test_stop(self):
        self.case_state = 1
        self.case_result['log'] = self.case_log
        self.result.append(self.case_result)