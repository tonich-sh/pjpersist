##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# Copyright (c) 2014 Shoobx, Inc.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Statistics on executed queries"""

from collections import namedtuple

QueryStats = namedtuple("QueryStats", ["query", "args", "time"])

QueryTotals = namedtuple('QueryTotals',
                         ["total_queries", "total_time", "sorted_queries"])


# Number of most expensive queries to print out
NUM_OF_QUERIES_TO_REPORT = 10


class QueryReport(object):
    def __init__(self):
        self.qlog = []

    def record(self, query, args, elapsed_time):
        """Record executed query

        elapsed_time is time, elapsed by executing query, in secodes
        """
        self.qlog.append(QueryStats(query, args, elapsed_time))

    def calc_totals(self):
        """Calculate totals and return QueryTotals object
        """

        total_queries = len(self.qlog)
        total_time = sum(q.time for q in self.qlog)
        sorted_queries = sorted(self.qlog, key=lambda q: q.time)
        return QueryTotals(total_queries, total_time, sorted_queries)

    def calc_and_report(self):
        """Calculate totals and print out report
        """
        if len(self.qlog) == 0:
            return "Query report: no queries were executed"
        totals = self.calc_totals()
        sep = '-' * 60

        report = []
        p = report.append

        p("Query report:")
        p(sep)
        p("%s most expensive queries:" % NUM_OF_QUERIES_TO_REPORT)
        for q in totals.sorted_queries[-NUM_OF_QUERIES_TO_REPORT:]:
            p("*** %s" % q.query)
            p("... ARGS: %s" % q.args)
            p("... TIME: %.4fms" % (q.time * 1000))
            p("")
        p(sep)
        p("Queries executed: %s" % totals.total_queries)
        p("Time spent: %.4fms" % (totals.total_time * 1000))

        return "\n".join(report)
