# file-data-loader

File based loader for Cloudant

* [Tests](#tests)
* [License](#license)

## Tests

To run the test suite first edit the cloudant properties. Create or open the file `src/test/resources/cloudant-account.properties` and `src/test/resources/cloudant-base.properties`, provide values for the following properties

~~~ cloudant-account.properties
cloudant.account=myCloudantAccount
cloudant.username=testuser
cloudant.password=testpassword
cloudant.database.prefix=prefix for test database that will be created/destroyed
~~~

Once all the required properties are provided in the properties file run `com.cloudant.test.com.cloudant.se.LoaderTestSuite` test class.

## License

Copyright 2015 Cloudant, an IBM company.

Licensed under the apache license, version 2.0 (the "license"); you may not use this file except in compliance with the license.  you may obtain a copy of the license at

    http://www.apache.org/licenses/LICENSE-2.0.html

Unless required by applicable law or agreed to in writing, software distributed under the license is distributed on an "as is" basis, without warranties or conditions of any kind, either express or implied. See the license for the specific language governing permissions and limitations under the license.

[query]: http://docs.cloudant.com/api/cloudant-query.html
[search]: http://docs.cloudant.com/api/search.html
[auth]: http://docs.cloudant.com/api/authz.html
[issues]: https://github.com/cloudant/java-cloudant /issues
[follow]: https://github.com/iriscouch/follow
[request]:  https://github.com/mikeal/request
