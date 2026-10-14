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
Every push to `main` publishes the `commons` and `client` jars, versioned `<build>-<commit>`, to Pennsieve's AWS CodeArtifact. Only Jenkins publishes; locally, `assume-role non-prod admin` lets you build against CodeArtifact.
