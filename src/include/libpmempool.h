/*
 * Copyright (c) 2016, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * libpmempool.h -- definitions of libpmempool entry points
 *
 * XXX
 *
 * See libpmempool(3) for details.
 */

#ifndef	LIBPMEMPOOL_H
#define	LIBPMEMPOOL_H 1

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Return values:
 *  0 - not pool set file
 *  1 - is pool set file
 * -1 - error while processing, sets errno appropriately
 */
int pmempool_is_poolset(const char *path);

/*
 *  0 - not on pmem-aware fs
 *  1 - on a pmem-aware fs
 * -1 - error while processing, sets errno appropriately
 */
int pmempool_is_pmem(const char *path);

/*
 * Removes all part files and replicas from the pool set file.
 * If specified path is not a pool set file, returns an error.
 *
 * I imagine we could add more functions like that:
 * - pmempool_set_chmod
 * - pmempool_set_chown
 * - pmempool_set_access
 * etc.
 */
int pmempool_set_remove(const char *path);

enum pmempool_type {
	PMEMPOOL_TYPE_LOG,
	PMEMPOOL_TYPE_BLK,
	PMEMPOOL_TYPE_OBJ,
	PMEMPOOL_TYPE_UNKNOWN,
};

/*
 * pmempool status:
 * - type of pool
 * - size of pool
 * - number of replicas
 * - is pool set file
 *
 * .. and maybe more like:
 * - major,
 * - compatibility features,
 * - crtime
 * etc.
 */
struct pmempool_stat {
	enum pmempool_type type;
	size_t size;
	size_t nreplicas;
	int is_poolset;
	int is_pmem;
};

int pmempool_stat(const char *path, struct pmempool_stat *buf);


/*
 * PMEMPOOL_MAJOR_VERSION and PMEMPOOL_MINOR_VERSION provide the current version
 * of the libpmempool API as provided by this header file.  Applications can
 * verify that the version available at run-time is compatible with the version
 * used at compile-time by passing these defines to pmempool_check_version().
 */
#define	PMEMPOOL_MAJOR_VERSION 1
#define	PMEMPOOL_MINOR_VERSION 0
const char *pmempool_check_version(
		unsigned major_required,
		unsigned minor_required);

const char *pmempool_errormsg(void);

#ifdef __cplusplus
}
#endif
#endif	/* libpmempool.h */
