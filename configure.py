import yaml
import os
import sys
import v20


class Config(object):
    """
    The Config object encapsulates all of the configuration required to create
    a v20 API context and configure it to work with a specific Account.

    Using the Config object enables the scripts to exist without many command
    line arguments (host, token, accountID, etc)
    """

    def __init__(self):
        """
        Initialize an empty Config object
        """
        self.hostname = None
        self.streaming_hostname = None
        self.port = 443
        self.ssl = True
        self.token = None
        self.username = None
        self.accounts = []
        self.active_account = None
        self.path = None
        self.datetime_format = "RFC3339"

    def __str__(self):
        """
        Create the string (YAML) representation of the Config instance
        """

        s = ""
        s += "hostname: {}\n".format(self.hostname)
        s += "streaming_hostname: {}\n".format(self.streaming_hostname)
        s += "port: {}\n".format(self.port)
        s += "ssl: {}\n".format(str(self.ssl).lower())
        s += "token: {}\n".format(self.token)
        s += "username: {}\n".format(self.username)
        s += "datetime_format: {}\n".format(self.datetime_format)
        s += "accounts:\n"
        for a in self.accounts:
            s += "- {}\n".format(a)
        s += "active_account: {}".format(self.active_account)

        return s

    def load(self, path=".v20.conf"):
        """
        Load the YAML config representation from a file into the Config
        instance

        Args:
            path: The location to read the config YAML from
        """

        self.path = path

        try:
            with open(os.path.expanduser(path)) as f:
                y = yaml.load(f, Loader=yaml.FullLoader)
                print(y)
                self.hostname = y.get("hostname", self.hostname)
                self.streaming_hostname = y.get("streaming_hostname",
                                                self.streaming_hostname)
                self.port = y.get("port", self.port)
                self.ssl = y.get("ssl", self.ssl)
                self.username = y.get("username", self.username)
                self.token = y.get("token", self.token)
                self.accounts = y.get("accounts", self.accounts)
                self.active_account = y.get("active_account",
                                            self.active_account)
                self.datetime_format = y.get("datetime_format",
                                             self.datetime_format)
        except Exception:
            raise ConfigPathError(path)
    
    def create_context(self):
        """
        Initialize an API context based on the Config instance
        """
        ctx = v20.Context(
            self.hostname,
            self.port,
            self.ssl,
            application="sample_code",
            token=self.token,
            datetime_format=self.datetime_format
        )

        return ctx

    def __str__(self):
        return f"Config({self.token,self.username,self.streaming_hostname,self.accounts})"

    def create_streaming_context(self):
        """
        Initialize a streaming API context based on the Config instance
        """
        # ctx_stream = v20.Context(hostname=self.streaming_hostname,
        #                  port=self.port, ssl=self.ssl,
        #                  token=self.token
        #                  )
        ctx = v20.Context(
            self.streaming_hostname,
            self.port,
            self.ssl,
            # application="sample_code",
            token=self.token,
            datetime_format=self.datetime_format
        )
        # print(ctx.pricing.stream(accountID=self.accounts[0],instruments='USD_EUR'))


        return ctx

        
class ConfigPathError(Exception):
    """
    Exception that indicates that the path specified for a v20 config file
    location doesn't exist
    """

    def __init__(self, path):
        self.path = path

    def __str__(self):
        return "Config file '{}' could not be loaded.".format(self.path)