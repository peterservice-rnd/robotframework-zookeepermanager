# -*- coding: utf-8 -*-

from JsonValidator import JsonValidator
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from robot.libraries.BuiltIn import BuiltIn
from robot.utils import ConnectionCache, PY3


class ZookeeperConnection(KazooClient):
    """KazooClient extended with zk_encoding"""

    def __init__(self, hosts, timeout, zk_encoding='utf-8'):
        """Initialize object.

        Args:\n
            _hosts_ - comma-separated list of zookeeper hosts to connect.\n
            _timeout_ - connection timeout in seconds.\n
            _zk_encoding_ - encoding of data in Zookeeper. \n
        """
        super(ZookeeperConnection, self).__init__(hosts, timeout)
        self.zk_encoding = zk_encoding


class ZookeeperManager(object):
    """
    Robot Framework library for working with Apache Zookeeper.
    Based on Kazoo Python library.
    All errors described in "Returns" section are kazoo exceptions (kazoo.exceptions).

    == Dependencies ==
    | robot framework   | http://robotframework.org           |
    | kazoo             | https://github.com/python-zk/kazoo  |
    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'

    def __init__(self):
        """Library initialization.
        Robot Framework ConnectionCache() class is prepared for working with concurrent connections."""

        self.bi = BuiltIn()
        self._connection = None
        self._cache = ConnectionCache()
        self._connections = []

    def connect_to_zookeeper(self, hosts, timeout=10, alias=None, zk_encoding='utf-8'):
        """
        Connection to Zookeeper

        *Args:*\n
            _hosts_ - comma-separated list of hosts to connect.\n
            _timeout_ - connection timeout in seconds. Default timeout is 10 seconds.\n
            _alias_ - alias of zookeeper connection. \n
            _zk_encoding_ - zookeeper encoding.\n

        *Returns:*\n
            Returns the index of the new connection. The connection is set as active.

        *Example:*\n
            | Connect To Zookeeper | host1:2181, host2 | 25 |
        """

        self._connection = ZookeeperConnection(hosts, timeout, zk_encoding=zk_encoding)
        self._connection.start()
        self._connections.append(self._connection)
        return self._cache.register(self._connection, alias)

    def disconnect_from_zookeeper(self):
        """
        Close current connection to Zookeeper

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Disconnect From Zookeeper |
        """

        self._connection.stop()
        self._connection.close()

    def switch_zookeeper_connection(self, index_or_alias):
        """
        Switch to another existing Zookeeper connection using its index or alias.\n

        The connection index is obtained on creating connection.
        Connection alias is optional and can be set at connecting to Zookeeper [#Connect To Zookeeper|Connect To Zookeeper].

        *Args:*\n
            _index_or_alias_ - index or alias of zookeeper connection. \n

        *Returns:*\n
            Index of previous connection.

        *Example:*\n
            | Connect To Zookeeper | host1:2181 | alias=zoo1 |
            | Switch Zookeeper Connection | zoo1 |
            | Get Value | /ps/config |
            | Connect To Zookeeper | host2:2181 | alias=zoo2 |
            | Switch Zookeeper Connection | zoo2 |
            | Get Value | /ps/config |
        """

        old_index = self._cache.current_index
        self._connection = self._cache.switch(index_or_alias)
        return old_index

    def close_all_zookeeper_connections(self):
        """
        Close all Zookeeper connections that were opened.
        After calling this keyword connection index returned by opening new connections [#Connect To Zookeeper |Connect To Zookeeper],
        starts from 1.
        You should not use [#Close all Zookeeper Connections | Close all Zookeeper Connections] and [#Disconnect From Zookeeper | Disconnect From Zookeeper]
        together.

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Close All Zookeeper Connections |
        """

        self._connection = self._cache.close_all(closer_method='stop')
        self._connection = self._cache.close_all(closer_method='close')

    def create_node(self, path, value='', force=False):
        """
        Create a node with a value.

        *Args:*\n
            _path_ - node path.\n
            _value_ - node value. Default is an empty string.\n
            _force_ - if TRUE parent path will be created. Default value is FALSE.\n

        *Raises:*\n
            _NodeExistError_ - node already exists.\n
            _NoNodeError_ - parent node doesn't exist.\n
            _NoChildrenForEphemeralsError_ - parent node is an ephemeral mode.\n
            _ZookeeperError_ - value is too large or server returns a non-zero error code.\n

        *Example:*\n
            | Create Node  |  /my/favorite/node  |  my_value |  ${TRUE} |
        """

        string_value = value.encode(self._connection.zk_encoding)
        self._connection.create(path, string_value, None, False, False, force)

    def delete_node(self, path, force=False):
        """
        Delete a node.

        *Args:*\n
            _path_ - node path.\n
            _force_ - if TRUE and node exists - recursively delete a node with all its children,
                      if TRUE and node doesn't exists - error won't be raised. Default value is FALSE.

        *Raises:*\n
            _NoNodeError_ - node doesn't exist.\n
            _NotEmptyError_ - node has children.\n
            _ZookeeperError_ - server returns a non-zero error code.
        """

        try:
            self._connection.delete(path, -1, force)
        except NoNodeError:
            if force:
                pass
            else:
                raise

    def exists(self, path):
        """
        Check if a node exists.

        *Args:*\n
            _path_ - node path.

        *Returns:*\n
            TRUE if a node exists, FALSE in other way.
        """

        node_stat = self._connection.exists(path)
        if node_stat is not None:
            # Node exists
            return True
        else:
            # Node doesn't exist
            return False

    def set_value(self, path, value, force=False):
        """
        Set the value of a node.

        *Args:*\n
            _path_ - node path.\n
            _value_ - new value.\n
            _force_ - if TRUE path will be created. Default value is FALSE.\n

        *Raises:*\n
            _NoNodeError - node doesn't exist.\n
            _ZookeeperError - value is too large or server returns non-zero error code.
        """

        string_value = value.encode(self._connection.zk_encoding)
        if force:
            self._connection.ensure_path(path)
        self._connection.set(path, string_value)

    def get_value(self, path):
        """
        Get the value of a node.

        *Args:*\n
            _path_ - node path.

        *Returns:*\n
            Value of a node.

        *Raises:*\n
            _NoNodeError_ - node doesn't exist.\n
            _ZookeeperError_ - server returns a non-zero error code.
        """

        value, _stat = self._connection.get(path)
        if PY3 and value is not None:
            return value.decode(self._connection.zk_encoding)
        return value

    def get_children(self, path):
        """
        Get a list of node's children.

        *Args:*\n
            _path_ - node path.

        *Returns:*\n
            List of node's children.

        *Raises:*\n
            _NoNodeError_ - node doesn't exist.\n
            _ZookeeperError_ - server returns a non-zero error code.
        """

        children = self._connection.get_children(path)
        return children

    def check_node_value_existence(self, path):
        """
        Check that value of node isn't empty.

        *Args:*\n
            _path_ - node path \n
        """
        value = self.get_value(path)
        if value in ("{}", "[]", "null"):
            raise Exception("Value of Zookeeper node {0} is equal {1}".format(path, value))
        elif not value:
            raise Exception("Value of Zookeeper node {0} is empty".format(path))
        else:
            BuiltIn().log("Zookeeper node {0} value is exist: '{1}' ".format(path, value))

    def check_json_value(self, path, expr):
        """
        Check that value of node matches JSONSelect expression

        *Args:*\n
            _path_ - node path \n
            _expr_ - JSONSelect expression \n

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Check Json Value | /my/favorite/node | .color:val("blue") |
        """

        json_string = self.get_value(path)
        JsonValidator().element_should_exist(json_string, expr)

    def check_node_value(self, path, value):
        """
        Check that value of the node is equal to the expected value

        *Args:*\n
            _path_ - node path \n
            _value_ - expected value \n

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Check Node Value | /my/favorite/node | 12345 |
        """

        node_value = self.get_value(path)
        if node_value == value:
            BuiltIn().log("Zookeeper node value is equal expected value: '{0}' ".format(value))
        else:
            raise Exception("Zookeeper node value is not equal expected value: '{0}' != '{1}' ".format(node_value, value))

    def safe_get_children(self, path):
        """
        Check that node exists and return its child nodes.

        *Args:*\n
            _path_ - node path \n

        *Returns:*\n
            List of node's children \n

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Safe Get Children | /my/favorite/node |
        """
        if self.exists(path):
            children = self.get_children(path)
            if children:
                return children
            raise Exception('There is no child for node {} in zookeeper'.format(path))
        raise Exception('There is no node {} in zookeeper'.format(path))

    def safe_get_value(self, path):
        """
        Check that node exists and return its value if not empty or null.

        *Args:*\n
            _path_ - node path \n

        *Returns:*\n
            Value of the node \n

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Safe Get Value | /my/favorite/node |
        """
        if self.exists(path):
            self.check_node_value_existence(path)
            return self.get_value(path)
        raise Exception('There is no node {} in zookeeper'.format(path))

    def check_zookeeper_list_availability(self, zookeeper_list, default_encoding='utf-8'):
        """
        Iterate over zookeeper instances from the list and check if connection to each can be created,
         after that return a list with results.

        *Args:*\n
            _zookeeper_list_: list of dictionaries with Zookeeper instances(name, host, port, zk_encoding (optional)) \n
            _default_encoding_: default zookeeper encoding. Used when there is no zk_encoding for Zookeeper instance
                                in zookeeper_list \n

        *Returns:*\n
            List of dictionaries with connection results for each Zookeeper instances

        *Example:*\n
            | &{ZOOKEEPER_1} | Create Dictionary | name=Zookeeper_1 | host=127.0.0.1 | port=2181 | zk_encoding=utf-8 |
            | &{ZOOKEEPER_2} | Create Dictionary | name=Zookeeper_2 | host=127.0.0.1 | port=2182 |
            | @{ZOOKEEPER_LIST} | Create List | &{ZOOKEEPER_1} | &{ZOOKEEPER_2} |
            | Check Zookeeper List Availability | zookeeper_list=${ZOOKEEPER_LIST} |
        """
        results = list()
        for zookeeper in zookeeper_list:
            result = dict()
            zookeeper_host = '{}:{}'.format(zookeeper['host'], zookeeper['port'])
            result['test'] = '{} {}'.format(zookeeper['name'].upper(), zookeeper_host)
            try:
                self.connect_to_zookeeper(hosts=zookeeper_host,
                                          zk_encoding=zookeeper.get('zk_encoding', default_encoding))
                result['success'] = True
            except Exception as ex:
                result['success'] = False
                result['reason'] = ex
            finally:
                self.close_all_zookeeper_connections()
                results.append(result)
        return results

    def check_zookeeper_nodes(self, zookeeper_nodes):
        """Iterate over a list with main zookeeper nodes, checking that each exists.

        *Args:*\n
            _zookeeper_nodes_ - list that contains zookeeper nodes \n

        *Example:*\n
            | Connect To Zookeeper | 127.0.0.1: 2181 |
            | Check Zookeeper Nodes | ['/my/favorite/node'] |

        """
        for node in zookeeper_nodes:
            node_exists = self.exists(node)
            message = "Node {0} doesn't exist.".format(node)
            self.bi.should_be_true(condition=node_exists,
                                   msg=message)
