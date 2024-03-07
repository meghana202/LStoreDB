import os

class DurablePageIDGenerator:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DurablePageIDGenerator, cls).__new__(cls)
            # Initialize any variables here that you don't want to be reinitialized
            # with each instantiation attempt. 'storage_path' is an example.
            cls._instance.storage_path = "./page_id.dat"
            cls._instance.current_id = cls._instance._load_last_id()
        return cls._instance

    def _load_last_id(self):
        # Attempt to read the last used ID from a file
        if os.path.exists(self.storage_path):
            with open(self.storage_path, 'r') as file:
                last_id = file.read()
                if last_id:
                    return int(last_id)
        return 0  # Default to 0 if file does not exist or is empty

    def _save_last_id(self):
        # Persist the current ID to a file
        with open(self.storage_path, 'w') as file:
            file.write(str(self.current_id))

    def get_new_id(self):
        # Increment the ID, save it, and return it
        self.current_id += 1
        self._save_last_id()
        return self.current_id


#print(f"{DurablePageIDGenerator().get_new_id()}")

