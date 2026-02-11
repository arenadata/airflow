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

S3 Gateway usage
================

S3 Gateway functionality in this provider uses its own internal S3-compatible client based on ``boto3``
and the ``ozone_s3`` connection type. It does **not** depend on the Amazon S3 hook/provider.

Connection
----------

Create an ``ozone_s3`` connection (for example ``ozone_s3_default``) and set:

* Extra ``endpoint_url`` (S3 Gateway URL), for example ``http://s3g:9878`` or ``https://s3g:9879``
* Extra ``verify`` (optional): ``false`` (development only) or a CA bundle path

Example (Extra)
^^^^^^^^^^^^^^^

.. code-block:: json

   {
     "endpoint_url": "https://s3g:9879",
     "verify": false
   }

Kerberos note
-------------

Kerberos settings in the ``ozone`` Native CLI connection do **not** apply to S3 Gateway calls.
S3 Gateway uses AWS Signature V4 authentication.
