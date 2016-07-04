### POOLSET SYNCHRONIZATION AND TRANSFORMATION  ###

To synchronize replicas within poolset using **libpmempool**,
in case of damaged single part or whole replica 'pmempool_sync'
function is used. For altering internal structure of poolset
in terms of joining or splitting parts, changing the path or name
of the part file 'pmempool_transform' is intended.
Both function return either 0 on succcess or -1 in case of error
with proper errno set accordingly.

```c
int pmempool_sync(
const char *poolset_path,
struct pmempool_replica_opts *opts);
```

The 'pmempool_sync()' takes path to poolset file 'poolset_path'
and pointer to option structure 'opts' defined as follows:

```c
struct pmempool_replica_opts {
	/* number of replica we copy from */
	unsigned replfrom;

	/* part number we copy from */
	int partfrom;

	/* number of replica we copy to */
	unsigned replto;

	/* part number we copy to */
	int partto;

	/* operational flags */
	unsigned flags;
};
```
The direction of data transfer is set by 'replfrom' and 'replto'
arguments. Replicas are numbered starting from 0 for primary replica.
Part number may be specified when transfer to/from specific part
is required. In this case part number has to be given either for
'partfrom' or 'partto' argument. The other part argument must be
set to -1. When both 'partfrom' and 'partto' equal to -1 whole
replica is copied.
Only 'PMEMPOOL_REPLICA_VERIFY' flag is supported
as a value of 'flags' argument. To see datils please navigate to
'POOLSET FLAGS' section

Following examples presents 'pmempool_replica_opts' setup for
copying whole replica and single part.

Copy whole replica 2 to primary replica 0:
```c
struct pmempool_replica_opts opts{
	.replfrom = 2;
	.partfrom = -1;
	.replto = 0;
	.partto = -1;
	.flags = 0;
};
```

Copy single part 2 from replica 1 to replica 3:
```c
struct pmempool_replica_opts opts{
	.replfrom = 1;
	.partfrom = 2;
	.replto = 3;
	.partto = -1;
	.flags = 0;
};
```

Replace single part 5 in replica 1 with part/s from replica 3:
```c
struct pmempool_replica_opts opts{
	.replfrom = 3;
	.partfrom = -1;
	.replto = 1;
	.partto = 5;
	.flags = 0;
};
```

```c
int pmempool_transform(
const char *poolset_in_path,
const char *poolset_out_path,
unsigned flags);
```

The 'pmempool_transform()' is currently **not implemented** and returns
'ENOSYS' errrno.
The 'pmempool_transform()' converts internal structure of poolset.
It allows to change localization of parts within replicas, rename parts
and split or join parts. Function takes two arguments of path to poolset
files and 'flags' that enables passing additional options to the function.
First 'poolset_in_path' provides the sources poolset to be changed.
'poolset_out_path' contains the target structure of poolset. Current
set of accepted flags contains 'PMEMPOOL_REPLICA_KEEP_ORIG'. Flag details
are described in 'POOLSET FLAGS' section.

### POOLSET FLAGS  ###

The 'flags' argument accepts any combination of the following values
defined for particular function.

The pmempool_sync() accepts following values:

'PMEMPOOL_REPLICA_VERIFY' - do not apply changes, only check
correctness of conversion

The pmempool_transform() accepts following values:

'PMEMPOOL_REPLICA_KEEP_ORIG' - when replica is renamed
or localization is changed keep the original location
