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

Naming & quota rules
====================

Volume and bucket naming
------------------------

Ozone volume and bucket names must match the pattern ``[a-z0-9.-]+`` (lowercase letters, digits, dots and dashes).
Underscores are not allowed.

Quotas
------

When a volume has a space quota, Ozone requires that buckets inside that volume also have a space quota.

In practice:

* If you create a volume with ``quota=...`` you should create buckets with ``quota=...`` too.
* If you set or update quota on a volume, consider setting bucket quotas as well.
