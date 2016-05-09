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
 * check_write.c -- write fixed data back
 */

#include <stdint.h>

#include "out.h"
#include "btt.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"
#include "check_write.h"

union location {
	struct {

		uint32_t step;
	};
	struct check_instep instep;
};

enum questions {
	Q_REPAIR_MAP,
	Q_REPAIR_FLOG,
};

/*
 * log_write -- (internal) write all structures for log pool
 */
static int
log_write(PMEMpoolcheck *ppc, union location *loc)
{
	if (!ppc->args.repair || ppc->args.dry_run)
		return 0;

	/* endianness conversion */
	struct pmemlog *log = &ppc->pool->hdr.log;
	log->start_offset = htole64(log->start_offset);
	log->end_offset = htole64(log->end_offset);
	log->write_offset = htole64(log->write_offset);


	if (pool_write(ppc->pool, log, sizeof(*log), 0)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_ERR(ppc, "writing pmemlog structure failed");
	}

	return 0;
}

/*
 * btt_flog_convert2le -- (internal) convert btt_flog to LE byte order
 */
static void
btt_flog_convert2le(struct btt_flog *flogp)
{
	flogp->lba = htole32(flogp->lba);
	flogp->old_map = htole32(flogp->old_map);
	flogp->new_map = htole32(flogp->new_map);
	flogp->seq = htole32(flogp->seq);
}

/*
 * blk_write_flog -- (internal) convert and write flog to file
 */
static int
blk_write_flog(PMEMpoolcheck *ppc, struct arena *arenap)
{
	if (!arenap->flog) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return CHECK_ERR(ppc, "flog is missing");
	}

	uint64_t flogoff = arenap->offset + arenap->btt_info.flogoff;

	uint8_t *ptr = arenap->flog;
	uint32_t i;
	for (i = 0; i < arenap->btt_info.nfree; i++) {
		struct btt_flog *flog_alpha = (struct btt_flog *)ptr;
		struct btt_flog *flog_beta = (struct btt_flog *)(ptr +
				sizeof(struct btt_flog));

		btt_flog_convert2le(flog_alpha);
		btt_flog_convert2le(flog_beta);

		ptr += BTT_FLOG_PAIR_ALIGN;
	}

	if (pool_write(ppc->pool, arenap->flog, arenap->flogsize, flogoff)) {
		CHECK_INFO(ppc, "%s", ppc->path);
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_ERR(ppc, "arena %u: writing BTT FLOG failed\n",
			arenap->id);
	}

	return 0;
}

/*
 * blk_write_map -- (internal) convert and write map to file
 */
static int
blk_write_map(PMEMpoolcheck *ppc, struct arena *arenap)
{
	if (!arenap->map) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return CHECK_ERR(ppc, "map is missing");
	}

	uint64_t mapoff = arenap->offset + arenap->btt_info.mapoff;

	uint32_t i;
	for (i = 0; i < arenap->btt_info.external_nlba; i++)
		arenap->map[i] = htole32(arenap->map[i]);

	if (pool_write(ppc->pool, arenap->map, arenap->mapsize, mapoff)) {
		CHECK_INFO(ppc, "%s", ppc->path);
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_ERR(ppc, "arena %u: writing BTT map failed\n",
			arenap->id);
	}

	return 0;
}

/*
 * blk_write -- (internal) write all structures for blk pool
 */
static int
blk_write(PMEMpoolcheck *ppc, union location *loc)
{
	if (!ppc->args.repair || ppc->args.dry_run)
		return 0;

	/* endianness conversion */
	ppc->pool->hdr.blk.bsize = htole32(ppc->pool->hdr.blk.bsize);

	if (pool_write(ppc->pool, &ppc->pool->hdr.blk,
			sizeof(ppc->pool->hdr.blk), 0)) {
		CHECK_INFO(ppc, "%s", ppc->path);
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_ERR(ppc, "writing pmemblk structure failed");
	}

	return 0;
}

/*
 * btt_data_write -- (internal) write BTT data
 */
static int
btt_data_write(PMEMpoolcheck *ppc, union location *loc)
{
	struct arena *arenap;

	TAILQ_FOREACH(arenap, &ppc->pool->arenas, next) {

		pool_btt_info_convert2le(&arenap->btt_info);

		if (ppc->pool->uuid_op == UUID_REGENERATED) {
			memcpy(arenap->btt_info.parent_uuid,
				ppc->pool->hdr.pool.poolset_uuid,
					sizeof(arenap->btt_info.parent_uuid));

			util_checksum(&arenap->btt_info,
					sizeof(arenap->btt_info),
					&arenap->btt_info.checksum, 1);
		}

		if (pool_write(ppc->pool, &arenap->btt_info,
				sizeof(arenap->btt_info), arenap->offset)) {
			CHECK_INFO(ppc, "%s", ppc->path);
			CHECK_ERR(ppc, "arena %u: writing BTT Info failed",
				arenap->id);
			goto error;
		}

		if (pool_write(ppc->pool, &arenap->btt_info,
				sizeof(arenap->btt_info), arenap->offset +
				le64toh(arenap->btt_info.infooff))) {
			CHECK_INFO(ppc, "%s", ppc->path);
			CHECK_ERR(ppc,
				"arena %u: writing BTT Info backup failed",
				arenap->id);
			goto error;
		}

		if (blk_write_flog(ppc, arenap))
			goto error;

		if (blk_write_map(ppc, arenap))
			goto error;
	}

	return 0;

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return -1;
}

struct step {
	int (*func)(PMEMpoolcheck *, union location *loc);
	enum pool_type type;
	int btt_dev;
};

static const struct step steps[] = {
	{
		.func		= log_write,
		.type		= POOL_TYPE_LOG,

	},
	{
		.func		= blk_write,
		.type		= POOL_TYPE_BLK,
	},
	{
		.func		= btt_data_write,
		.type		= POOL_TYPE_BLK,
		.btt_dev	= true

	},
	{
		.func		= NULL,

	},
};

/*
 * step_exe -- (internal) perform single step according to its parameters
 */
static inline int
step_exe(PMEMpoolcheck *ppc, union location *loc)
{
	const struct step *step = &steps[loc->step++];

	/* check step conditions */
	if (!(step->btt_dev && ppc->pool->params.is_btt_dev))
		if (!(step->type & ppc->pool->params.type))
			return 0;

	return step->func(ppc, loc);
}

/*
 * check_write -- write fixed data back
 */
void
check_write(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof(union location) !=
		sizeof(struct check_instep));

	union location *loc = (union location *)check_step_location(ppc->data);

	/* do all steps */
	while (loc->step != CHECK_STEP_COMPLETE &&
		steps[loc->step].func != NULL) {

		if (step_exe(ppc, loc))
			return;
	}
}
