# job-scheduling-service
ETL job-scheduler, state-manager, and restful interface with scala client

## Test Coverage
We use [scoverage](http://scoverage.org/) to measure our test coverage.

To view test coverage:

```
sbt> coverage
sbt> test
sbt> coverageReport
```

## Releasing the Client
The client is built and published to Pennsieve's Nexus repository on every push to the `master` branch.

### SNAPSHOT RELEASE
Run `sbt publish` to publish the current snapshot version to Pennsieve's nexus repository
