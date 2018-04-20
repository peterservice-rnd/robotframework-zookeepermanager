RobotFramework Zookeeper Manager Library
========================================

|Build Status|

Short Description
-----------------

`Robot Framework`_ library for managing Apache Zookeeper, based on `kazoo`_ python library.

Installation
------------

::

    pip install robotframework-zookeepermanager

Documentation
-------------

See keyword documentation for robotframework-zookeepermanager library in folder ``docs``.

Example
-------
+-----------+------------------+
| Settings  |      Value       |
+===========+==================+
|  Library  | ZookeeperManager |
+-----------+------------------+

+---------------+---------------------------------+-------------------+-------------------+----------+
|  Test cases   |               Action            |     Argument      |      Argument     | Argument |
+===============+=================================+===================+===================+==========+
|  Simple Test  | Connect To Zookeeper            | 127.0.0.1: 2181   |                   |          |
+---------------+---------------------------------+-------------------+-------------------+----------+
|               | Create Node                     | /my/favorite/node | my_value          | ${TRUE}  |
+---------------+---------------------------------+-------------------+-------------------+----------+
|               | ${node_exists}=                 | Exists            | /my/favorite/node |          |
+---------------+---------------------------------+-------------------+-------------------+----------+
|               | Delete Node                     | /my/favorite/node |                   |          |
+---------------+---------------------------------+-------------------+-------------------+----------+
|               | Close All Zookeeper Connections |                   |                   |          |
+---------------+---------------------------------+-------------------+-------------------+----------+

License
-------

Apache License 2.0

.. _Robot Framework: http://www.robotframework.org
.. _kazoo: https://github.com/python-zk/kazoo

.. |Build Status| image:: https://travis-ci.org/peterservice-rnd/robotframework-zookeepermanager.svg?branch=master
   :target: https://travis-ci.org/peterservice-rnd/robotframework-zookeepermanager