import pandas as pd

class ParserModule:
    def __init__(self, data = None):
        self._data = data
    

    def load_data(self):
        # This method should be overridden by subclasses to load data
        pass


    def parse_data(self):
        # This method should be overridden by subclasses to preprocess data
        pass


    def get_data(self):
        return self._data
    

    def set_data(self, data):
        self._data = data
    
    
    def to_dataframe(self, data = None):

        """
        Converts the parsed data to a pandas DataFrame.
        
        Returns:
            pd.DataFrame: The parsed data as a pandas DataFrame.
        """
        
        if data is not None:
            self.set_data(data)

        if self._data is None:
            raise ValueError("No data available to convert to DataFrame.")
        
        if isinstance(self._data, pd.DataFrame):
            return self._data
        else:
            return pd.DataFrame(self._data)

    
    def save_data(self, file_path):

        """
        Saves the parsed data to a specified file path.
        
        Args:
            file_path (str): The path where the data should be saved.
        """

        if not isinstance(self._data, pd.DataFrame):
            raise TypeError("Data must be a pandas DataFrame to save as CSV.")

        if self._data is not None:
            self._data.to_csv(file_path, index=False)
        else:
            raise ValueError("No data to save.")
        

    def load_data_from_csv(self, file_path):

        """
        Loads data from a CSV file into the parser module.
        
        Args:
            file_path (str): The path to the CSV file to load.
        
        Returns:
            pd.DataFrame: The loaded data as a pandas DataFrame.
        """

        self._data = pd.read_csv(file_path)
        return self._data