# A Hybrid Multi-Query Optimization Technique Combining Sub-Expression and Materialized View Reuse

This repo contains the accompanying code for the Master's thesis. It uses Apache Calcite for performing database optimizations. Currently, PostgreSQL is supported but because of the extensible nature of Calcite, other databases can also be added.

The code is divided into three main packages: `batch`, `cache`, and `mv`.

- `batch` handles the generation of shared query plans when multiple queries are given.
- `cache` contains the code for implementing a caching mechanism, along with a replacement policy.
- `mv` enables materialized view substitutions for the given queries from materialized views present in the cache.

Other packages that provide utility are:

- `test`, which generates suitable queryloads for testing the hybrid mqo method.
- `common`, which contains commonly used utility functions.

Additionally, `Window.java` composes the functionality and provides a single interface to execute a given queryload. `Tester.java` contains methods for evaluating meta-information about the hybrid mqo method such as the number of derivable queries, and is also responsible for running tests.
