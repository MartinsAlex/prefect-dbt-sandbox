
from prefect.blocks.core import Block
from prefect.blocks.core import Block
from pydantic import SecretStr
from typing import Optional
from prefect.blocks.system import JSON

class DatabaseConnection(Block):
    """
    A class that stores attributes necessary for connecting to a database.
    
    Attributes:
    - host (str): The hostname or IP address of the database server.
    - port (int): The port number where the database server is listening.
    - username (str): The username for authentication.
    - password (SecretStr): The password for authentication.
    - database_name (str): The name of the specific database to connect to.
    - additional_params (dict): Any additional parameters specific to a connector type.
    """
    
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database_name: Optional[str] = None
    additional_params: Optional[JSON] = None
    
    def __repr__(self):
        """Representation of the DatabaseConnection object for debugging purposes."""
        return (f"<DatabaseConnection(host={self.host}, port={self.port}, "
                f"username={self.username}, database_name={self.database_name}, "
                f"connector_type={self.connector_type})>")
