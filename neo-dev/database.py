"""A database encapsulating collections of near-Earth objects and their close approaches.

A `NEODatabase` holds an interconnected data set of NEOs and close approaches.
It provides methods to fetch an NEO by primary designation or by name, as well
as a method to query the set of close approaches that match a collection of
user-specified criteria.

"""

class NEODatabase:
    def __init__(self, id, created_at, name, close_apprach_date, miss_distance):
        """Create a new `NEODatabase`.
        """
        self._id = id
        self.created_at = created_at
        self.name = name
        self.close_approach_date = close_approach_date
        self.miss_distance = miss_distance

    @classmethod
    def get_neo(self, neo):
        """Find and return an NEO by its primary designation.
        """
        return NEODatabase(neo['created_at'],
                           neo['id'],
                           neo['name'],
                           neo['close_approach_date'],
                           neo['miss_distance']
                           )
