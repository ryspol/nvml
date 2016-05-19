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

/*
 * libpmempool_replica.c -- libpmempool_replica_restore entry point
 */

#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "out.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "memops.h"
#include "pmalloc.h"
#include "list.h"
#include "libpmempool.h"

#include "libpmempool_replica.h"
#include "replica_sync.h"

/*
 * pmempool_sync -- copy one replica to another
 */
enum pmempool_replica_result
pmempool_sync(const char *poolset_in_path, unsigned repl_to,
		unsigned repl_from, uint32_t flags)
{
	enum pmempool_replica_result result = REPLICA_RES_INTERNAL_ERR;

	if (poolset_in_path == NULL) {
		ERR("poolset paths can not be NULL");
		return REPLICA_RES_WRONG_ARG;
	}

	/* check if poolset has correct signature */
	if (util_is_poolset(poolset_in_path) != 1)
		return REPLICA_RES_IN_POOLSET_ERR;

	/* open poolset file */
	int fd_in = util_file_open(poolset_in_path, NULL, 0, O_RDONLY);
	if (fd_in < 0)
		return REPLICA_RES_IN_POOLSET_ERR;

	struct pool_set *set_in = NULL;
	/* fill up pool_set structure */
	if (util_poolset_parse(poolset_in_path, fd_in, &set_in)) {
		ERR("Parsing input poolset file failed");
		result = REPLICA_RES_IN_POOLSET_ERR;
		goto err_close;
	}

	/* copy one replica to another */
	result = sync_replica(set_in, repl_to, repl_from, flags);

err_close:
	util_poolset_free(set_in);
	close(fd_in);
	return result;
}

/*
 * pmempool_convert -- alter poolset structure
 */
enum pmempool_replica_result
pmempool_convert(const char *poolset_in_path, const char *poolset_out_path,
		uint32_t flags)
{
	enum pmempool_replica_result result = REPLICA_RES_INTERNAL_ERR;

	if (poolset_in_path == NULL || poolset_out_path == NULL) {
		ERR("poolset paths can not be NULL");
		return REPLICA_RES_WRONG_ARG;
	}

	if (util_is_poolset(poolset_in_path) != 1)
		return REPLICA_RES_IN_POOLSET_ERR;
	if (util_is_poolset(poolset_out_path) != 1)
		return REPLICA_RES_OUT_POOLSET_ERR;
	int fd_in = util_file_open(poolset_in_path, NULL, 0, O_RDONLY);
	if (fd_in < 0)
		return REPLICA_RES_IN_POOLSET_ERR;
	int fd_out = util_file_open(poolset_out_path, NULL, 0, O_RDONLY);
	if (fd_out < 0) {
		result = REPLICA_RES_OUT_POOLSET_ERR;
		goto err_close_fin;
	}

	struct pool_set *set_in = NULL;
	struct pool_set *set_out = NULL;

	/* parse input poolset file */
	if (util_poolset_parse(poolset_in_path, fd_in, &set_in)) {
		ERR("Parsing input poolset file failed");
		result = REPLICA_RES_IN_POOLSET_ERR;
		goto err_close_finout;
	}

	/* parse output poolset file */
	if (util_poolset_parse(poolset_out_path, fd_out, &set_out)) {
		ERR("Parsing output poolset file failed");
		result = REPLICA_RES_OUT_POOLSET_ERR;
		goto err_close_poolinfree;
	}

	ERR("Only restore mode is available now");

	util_poolset_free(set_out);
err_close_poolinfree:
	util_poolset_free(set_in);
err_close_finout:
	close(fd_out);
err_close_fin:
	close(fd_in);
	return result;
}
