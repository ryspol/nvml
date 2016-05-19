/*
 * Copyright 2016, Intel Corporation
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
 *     * Neither the name of the copyright holder nor the names of its
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


#ifndef	LIBPMEMPOOL_REPLICA_H
#define	LIBPMEMPOOL_REPLICA_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

/* do not apply changes, only check correctness of conversion */
#define PMEMPOOL_REPLICA_VERIFY (1 << 0)
/*
 * when replica is renamed or localization is changed keep the original
 * version of replica
 */
#define PMEMPOOL_REPLICA_KEEPORIG (1 << 1)
/* truncate data when source replica has bigger size then target replica */
#define PMEMPOOL_REPLICA_TRUNCATE (1 << 2)

enum pmempool_replica_result {
	REPLICA_RES_INTERNAL_ERR,	/* internal error */
	REPLICA_RES_WRONG_ARG,		/* wrong argument */
	REPLICA_RES_WRONG_REPL_NUM,	/* no such replica number in poolset */
	REPLICA_RES_PART_FILE_DEL_ERR,	/* cannot remove part file */
	REPLICA_RES_PART_FILE_CREATE_ERR,	/* cannot create part file */
	REPLICA_RES_REP_CREATE_ERR,		/* cannot create replica */
	REPLICA_RES_REP_OPEN_ERR,		/* cannot open replica */
	REPLICA_RES_INSUF_TARGET_MEM,	/* not enough memory to copy replica */
	REPLICA_RES_CANNOT_UUIDS_UPDATE,	/* cannot update uuids */
	REPLICA_RES_COPY_SUCCESSFUL,	/* replica copied successful */
	REPLICA_RES_IN_POOLSET_ERR,	/* input poolset is not correct */
	REPLICA_RES_OUT_POOLSET_ERR,	/* output poolset is not correct */
	REPLICA_RES_CONVERT_ERR,	/* conversion of poolset failed */
	REPLICA_RES_CONVERT_OK	/* conversion of poolset succeeded */
};

enum pmempool_replica_result pmempool_sync(const char *poolset_in_path,
		unsigned repl_to, unsigned repl_from, uint32_t flags);

enum pmempool_replica_result pmempool_convert(const char *poolset_in_path,
		const char *poolset_out_path, uint32_t flags);

#ifdef __cplusplus
}
#endif
#endif	/* libpmempool_replica.h */
