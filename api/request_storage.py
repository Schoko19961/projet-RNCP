
import pandas as pd


class RequestStorage:
    """
        Class which stores the last 4 request results. Adding a fifth request result will remove the oldest one.
        It has a function to return the combined dataframe for a given date.
    """

    def __init__(self):
        self.request_storage: list(pd.DataFrame) = []
        self.max_storage = 4

    def add_request(self, request):
        if len(self.request_storage) >= self.max_storage:
            self.request_storage.pop(0)
        self.request_storage.append(request)

    def get_combined_dataframe(self):
        return pd.concat(self.request_storage)
    
