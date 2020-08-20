"""Setup module for Robot Framework Apache Zookeeper Manager Library package."""

# To use a consistent encoding
from codecs import open
from os import path

from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='robotframework-zookeepermanager',
    version='2.0.2',
    description='Robot Framework library for working with Apache Zookeeper.',
    long_description=long_description,
    url='https://github.com/peterservice-rnd/robotframework-zookeepermanager',
    author='Nexign',
    author_email='MF_AIST_all@nexign-systems.com',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Framework :: Robot Framework :: Library',
    ],
    keywords='testing testautomation robotframework autotest zookeeper',
    package_dir={'': 'src'},
    py_modules=['ZookeeperManager'],
    install_requires=[
        "kazoo==2.8.0",
        "robotframework",
        "robotframework-jsonvalidator"
    ],
)
