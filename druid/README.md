## Druid API Wrapper

Sample code and configuration for exposing a subset of Apache Druid API from nginx-aggr-module.
Can be used to visualize nginx-aggr-module data using applications like Turnilo/Swiv.

### Setup

* Install php-fpm & php-curl
* Compile nginx-aggr-module and start it with the provided sample conf
* Define the data sources in druidWrapper.json - can include multiple nginx-aggr-module schemas and multiple Druid endpoints.
* Configure Turnilo/Swiv to connect to port 8082 on the nginx-aggr-module server
