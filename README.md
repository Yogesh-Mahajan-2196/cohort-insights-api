## Schema & Staleness Design

Each document contains a monotonically increasing `content_version`
and a SHA-256 `content_hash`.

The original document content and its derived processing/enrichment
results are stored together, but each derived stage also records the
`content_version` it was generated from.

For example:

document.content_version = 3

processing.content_version = 3
enriching.content_version = 3

The API only exposes `summary` when:

processing.status = completed
AND
processing.content_version == document.content_version

Similarly, tags are only exposed when:

enriching.status = completed
AND
enriching.content_version == document.content_version

Therefore, when a PATCH occurs, the document version is incremented
and the previous derived fields are cleared immediately.

A worker processing version 2 cannot overwrite version 3 because
every worker update includes the expected `content_version` in its
MongoDB filter.

For example:

{
    "_id": document_id,
    "content_version": 2
}

If the document has already moved to version 3, MongoDB modifies zero
documents and the old worker result is discarded.

This prevents a mixed-version response such as content version 3 with
a summary generated from version 2.

The API therefore does not rely on a boolean `is_stale` field as the
source of truth. `is_stale` is derived from the version comparison.


## At 100×

### User-level indexes

If a single user has 500K documents, queries must avoid scanning the
entire collection. The main user listing query is:

user_id + created_at

with an index:

{ user_id: 1, created_at: -1 }

The optional status filter is supported by:

{ user_id: 1, status: 1, created_at: -1 }

This allows MongoDB to efficiently retrieve the newest documents for
a user without scanning unrelated users' documents.

### Horizontal scaling

MongoDB can be horizontally scaled using sharding. A natural shard
key candidate is user_id because most document access is scoped to a
user. In a large deployment, the exact shard key would be validated
against the actual distribution of users and workload because a small
number of extremely large users could create hotspots.

### Redis rate limiting

The per-user active-job limit is stored in Redis. Reservation is
performed atomically using a Redis Lua script. This prevents multiple
API instances from simultaneously allowing more than the configured
maximum of three active documents.

Because the counter is stored in Redis rather than application memory,
multiple API instances share the same concurrency limit.

### Pagination

The current implementation uses skip/limit pagination and an index
supporting user_id + created_at.

At very large offsets, skip/limit becomes increasingly expensive
because MongoDB still has to walk past earlier records. A production
alternative would be cursor-based pagination using the last
created_at value plus the document _id as a tie-breaker.

For example:

created_at < last_created_at

with a compound index:

{ user_id: 1, created_at: -1, _id: -1 }

This provides more stable performance for deep pagination.


------------------------------------------------

## Assumptions

- `X-User-Id` simulates the authenticated user identity because the
  assignment does not require implementation of a complete
  authentication system.
- MongoDB is the source of truth.
- Redis is used for active-job coordination, content caching, and the
  processing stream.
- A document can have at most three active processing jobs per user.
- `client_doc_ref` is unique when provided.
- Repeating the same `client_doc_ref` with the same content is treated
  as an idempotent retry.
- Reusing a `client_doc_ref` with different content returns HTTP 409.
- Processing and enrichment failures are simulated at approximately
  10%.
- Processing and enrichment durations are intentionally simulated.