# First Chapter 2 implementation

Start with a fair-loss transport boundary and Algorithm 2.1, stubborn point-to-point links. A sender keeps each accepted message and periodically sends it again. A receiver delivers each arrival, including duplicates. The fair-loss assumption concerns infinitely repeated sends between correct processes. A finite run can demonstrate recovery after a chosen loss interval.

| Work item | Acceptance evidence |
| --- | --- |
| Define the send request and delivery indication in `link/`. Keep sender, destination and message identity in each test history. | Unit tests cover invalid peer input, payload ownership across retries, and delivery through the FIFO dispatcher. |
| Implement retransmission with an injected clock. Retain pending messages in deterministic order. | Advance virtual time twice and inspect outgoing messages. A mutation to the caller's original buffer after Send must leave retry bytes intact. |
| Compose two nodes over simulated transport. Partition the direction from sender to receiver before sending. Heal it at a known time and run to a finite deadline. | Record silence during the partition and at least two deliveries of the same message after healing with loss disabled. Check every delivery against the sent history after each step. |
| Expose module-owned counters for first sends, retries and deliveries plus a gauge of retained messages. | Check counts against independent event histories and compare snapshots across replay. Continued retries after delivery are expected. |
| Register a scenario and starter config with a bounded duration. | Run the story for the new config. Its verdict rows name the peer, message and fault window being checked. End by time because retransmission keeps scheduling events. |
| Verify the public workflow. | Exercise config → built CLI → CSV → report, add a query for retry and delivery counts, and update the README package and suite maps. Run the standard gates in AGENTS.md. |

Algorithm 2.2 adds duplicate suppression above this link. Preserve the duplicate deliveries in the lower-layer test so the next module has an observable behavior to filter.
