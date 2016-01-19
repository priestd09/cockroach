# Redis testdata format

Test cases start with `redis>`.
Arguments are separated by spaces and may be grouped by double quotes.
This allows examples from redis.io to be copied verbatim.

Results follow on the next line until the following `redis>`.
They are in the same format that the `redis-cli` command produces.

Empty lines are ignored.
Comments are lines that begin with `#`.

## Operation

A `flushall` command is run before the start of each testdata file.
