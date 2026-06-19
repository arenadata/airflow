.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Package ``apache-airflow-providers-arenadata-ozone``
====================================================

Release: ``1.1.0``

Provider package for Apache Ozone native CLI workflows in Airflow.
All classes for this provider package are in the
``airflow.providers.arenadata.ozone`` Python package.

Current scope
-------------

This provider supports native Ozone CLI scenarios only:

* Ozone administration workflows via ``ozone sh``;
* Ozone filesystem workflows via ``ozone fs`` for ``ofs://`` and ``o3fs://`` paths;
* Ozone sensors and HDFS to Ozone transfer helpers;
* SSL and Kerberos runtime configuration through the Airflow connection extras.

The provider does not include an embedded S3 API layer. Use the standard Amazon
provider for Ozone S3 Gateway scenarios.

Installation
------------

Install this package on top of an existing Airflow installation with:

.. code-block:: bash

    pip install apache-airflow-providers-arenadata-ozone

Requirements
------------

===================  ================
PIP package          Version required
===================  ================
``apache-airflow``   ``>=3.2.1``
===================  ================
