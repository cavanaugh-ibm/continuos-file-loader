# file-data-loader

File based loader for Cloudant

Note - this exists as its own project because it uses Java 7+ features and the other projects must stay at Java 6 for specific customers

* [Tests](#tests)
* [Open Tasks](#open-tasks)
* [Completed Tasks](#completed-tasks)
* [License](#license)

## Tests

To run the tests the following properties must be set.

The following can come from either system properties, environment or the default configuration file `src/test/resources/cloudant.properties`

* cloudant_test_account=account
* cloudant_test_database_prefix=database
* cloudant_test_user=user

The following can come from either system properties, environment but NOT the configuration file

* cloudant_test_password=password

`mvn test`

## Open Tasks
* Add in version logic
* Add in end-2-end tests

## Completed Tasks
* Add flag to merge or replace

## License

Copyright 2015 Cloudant, an IBM company.

Licensed under the apache license, version 2.0 (the "license"); you may not use this file except in compliance with the license.  you may obtain a copy of the license at

    http://www.apache.org/licenses/LICENSE-2.0.html

Unless required by applicable law or agreed to in writing, software distributed under the license is distributed on an "as is" basis, without warranties or conditions of any kind, either express or implied. See the license for the specific language governing permissions and limitations under the license.

## Links

* [Issue Tracking](https://github.com/cavanaugh-ibm/file-data-loader/issues)
* [SE Common](https://github.com/cavanaugh-ibm/se-common)
* [Cloudant Query Docs](http://docs.cloudant.com/api/cloudant-query.html)
* [Cloudant Search Docs](http://docs.cloudant.com/api/search.html)
* [Cloudant Auth Docs](http://docs.cloudant.com/api/authz.html)
* [Cloudant Changes Follower](https://github.com/iriscouch/follow)
