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


class AdaptiveCache(object):

    def __init__(self, data):
        self.data = data

    def get(self, key):
        return self.data.get(key, '')

    def put(self, key, value):
        self.data[key] = value

    def has(self, key):
        return key in self.data

    def __getitem__(self, item):
        return self.data[item]

    def __contains__(self, item):
        return item in self.data
