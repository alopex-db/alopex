# First READ_ONLY begin: extension page diagnostic

Run 42408723037 observed one additional minor fault in every candidate first
READ_ONLY noop begin, together with higher caller CPU time. This diagnostic
tests whether first access to mapped native-extension pages accounts for that
difference. It does not qualify release performance or replace prior runs.

Reuse the verified before/candidate wheels, image and VM/container boundary from
the phase diagnostic. Keep 1000 rows, no indexes and three alternating-order
trials per subject. Each of the six cells has a separate seed process and two
fresh readers. Both readers perform exactly one READ_ONLY begin and rollback:

1. Natural first begin, with no product-code warm-up.
2. First begin after reading one byte from every readable mapped page of the
   native extension. This touches no transaction API and changes no protection,
   capability, mount, kernel setting, or file content.

Record the existing elapsed/CPU/fault/I/O observations and extension page presence
before/after begin. Read only this process's `maps` and `pagemap`. Use the present
bit, ignoring PFNs. Record software file offsets and permissions, not virtual
addresses or machine details. Derive page size at runtime and bound readable
native mappings to 128 MB. Permission or partial-read errors fail the diagnostic.

Allocate and touch observer buffers before measurement. Retain a no-op control
and reject extension-page changes caused by that control or background activity.
The prefault condition must make all observed pages present and retain that state
until begin. Kernel fault-around may install several PTEs per fault; do not equate
newly-present page count with minor-fault count. Timings with instrumentation are
not interchangeable with the original acceptance timings.

The independent auditor verifies the two conditions separately, original source
identities, extension hash, all sample and mapping fields, SQL final state,
collection hashes, and deleted owned resources. Relate software offsets to the
symbols already present in the original wheel. A page can contain several
functions; symbol overlap alone is not proof that a particular function caused
the fault.

Keep the existing 300-second child and 420-second cell deadlines, a 3600-second
matrix deadline, capacity guards, encrypted evidence and owned-resource cleanup.
There is no automatic retry and no performance-acceptance claim.

Access semantics: [Linux pagemap documentation](https://www.kernel.org/doc/html/latest/admin-guide/mm/pagemap.html)
and [proc_pid_pagemap](https://man7.org/linux/man-pages/man5/proc_pid_pagemap.5.html).
