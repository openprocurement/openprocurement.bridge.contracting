# -*- coding: utf-8 -*-


class MockedResponse(object):

    def __init__(self, status_int, text=None, headers=None, body_string=None):
        self.status_int = status_int
        self.text = text
        self.headers = headers
        self.string = body_string

    def body_string(self):
        return self.string


class AlmostAlwaysTrue(object):

    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)

