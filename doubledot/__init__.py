__version__ = "0.0.1"
# from crema_sf import Salesforce
# from ATMS_api import ATMS_api
__all__ = ["Salesforce","ATMS"]

# this is the magic syntax that allows you to import all the classes in the package
from .crema_sf import Salesforce
from .ATMS_api import ATMS_api as ATMS   


