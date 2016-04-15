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
 * check_btt_info.c -- check btt info
 */

#include <unistd.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/param.h>

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
	struct check_instep_location instep;
};

enum questions {
	Q_REPAIR_MAP,
	Q_REPAIR_FLOG,
};

/*
 * log_write -- write all structures for log pool
 */
static struct check_status *
log_write(PMEMpoolcheck *ppc, union location *loc)
{
	if (!ppc->repair || ppc->dry_run)
		return NULL;

	/* endianness conversion */
	ppc->pool->hdr.log.start_offset = htole64(ppc->pool->hdr.log.start_offset);
	ppc->pool->hdr.log.end_offset = htole64(ppc->pool->hdr.log.end_offset);
	ppc->pool->hdr.log.write_offset = htole64(ppc->pool->hdr.log.write_offset);


	if (pool_write(ppc->pool->set_file, &ppc->pool->hdr.log, sizeof (ppc->pool->hdr.log), 0)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_STATUS_ERR(ppc, "writing pmemlog structure failed");
	}

	return NULL;
}

/*
 * btt_flog_convert2le -- convert btt_flog to LE byte order
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
 * blk_write_flog -- convert and write flog to file
 */
static struct check_status *
blk_write_flog(PMEMpoolcheck *ppc, struct arena *arenap)
{
	if (!arenap->flog) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return CHECK_STATUS_ERR(ppc, "flog is missing");
	}

	uint64_t flogoff = arenap->offset + arenap->btt_info.flogoff;

	uint8_t *ptr = arenap->flog;
	uint32_t i;
	for (i = 0; i < arenap->btt_info.nfree; i++) {
		struct btt_flog *flog_alpha = (struct btt_flog *)ptr;
		struct btt_flog *flog_beta = (struct btt_flog *)(ptr +
				sizeof (struct btt_flog));

		btt_flog_convert2le(flog_alpha);
		btt_flog_convert2le(flog_beta);

		ptr += BTT_FLOG_PAIR_ALIGN;
	}

	if (pool_write(ppc->pool->set_file, arenap->flog, arenap->flogsize, flogoff)) {
		/*if (errno)
			warn("%s", ppc->fname);*/
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_STATUS_ERR(ppc, "arena %u: writing BTT FLOG failed\n", arenap->id);
	}

	return NULL;;
}

/*
 * blk_write_map -- convert and write map to file
 */
static struct check_status *
blk_write_map(PMEMpoolcheck *ppc, struct arena *arenap)
{
	if (!arenap->map) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return CHECK_STATUS_ERR(ppc, "map is missing");
	}

	uint64_t mapoff = arenap->offset + arenap->btt_info.mapoff;

	uint32_t i;
	for (i = 0; i < arenap->btt_info.external_nlba; i++)
		arenap->map[i] = htole32(arenap->map[i]);

	if (pool_write(ppc->pool->set_file, arenap->map, arenap->mapsize, mapoff)) {
		/*if (errno)
			warn("%s", ppc->fname);*/
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_STATUS_ERR(ppc, "arena %u: writing BTT map failed\n", arenap->id);
	}

	return NULL;
}

/*
 * btt_info_convert2le -- convert btt_info header to LE byte order
 */
static void
btt_info_convert2le(struct btt_info *infop)
{
	infop->flags = htole64(infop->flags);
	infop->minor = htole16(infop->minor);
	infop->external_lbasize = htole32(infop->external_lbasize);
	infop->external_nlba = htole32(infop->external_nlba);
	infop->internal_lbasize = htole32(infop->internal_lbasize);
	infop->internal_nlba = htole32(infop->internal_nlba);
	infop->nfree = htole32(infop->nfree);
	infop->infosize = htole32(infop->infosize);
	infop->nextoff = htole64(infop->nextoff);
	infop->dataoff = htole64(infop->dataoff);
	infop->mapoff = htole64(infop->mapoff);
	infop->flogoff = htole64(infop->flogoff);
	infop->infooff = htole64(infop->infooff);
	infop->checksum = htole64(infop->checksum);
}

/*
 * blk_write -- write all structures for blk pool
 */
static struct check_status *
blk_write(PMEMpoolcheck *ppc, union location *loc)
{
	if (!ppc->repair || ppc->dry_run)
		return NULL;

	struct check_status *status = NULL;

	/* endianness conversion */
	ppc->pool->hdr.blk.bsize = htole32(ppc->pool->hdr.blk.bsize);

	if (pool_write(ppc->pool->set_file, &ppc->pool->hdr.blk, sizeof (ppc->pool->hdr.blk), 0)) {
		/*if (errno)
			warn("%s", ppc->fname);*/
		status = CHECK_STATUS_ERR(ppc, "writing pmemblk structure failed");
		goto error;
	}

	struct arena *arenap;
	TAILQ_FOREACH(arenap, &ppc->pool->arenas, next) {

		btt_info_convert2le(&arenap->btt_info);

		if (ppc->pool->uuid_op == UUID_REGENERATED) {
			memcpy(arenap->btt_info.parent_uuid,
				ppc->pool->hdr.pool.poolset_uuid,
					sizeof (arenap->btt_info.parent_uuid));

			util_checksum(&arenap->btt_info,
					sizeof (arenap->btt_info),
					&arenap->btt_info.checksum, 1);
		}

		if (pool_write(ppc->pool->set_file, &arenap->btt_info,
			sizeof (arenap->btt_info), arenap->offset)) {
			/*if (errno)
				warn("%s", ppc->fname);*/
			status = CHECK_STATUS_ERR(ppc, "arena %u: writing BTT Info failed",
				arenap->id);
			goto error;
		}

		if (pool_write(ppc->pool->set_file, &arenap->btt_info,
			sizeof (arenap->btt_info), arenap->offset +
				le64toh(arenap->btt_info.infooff))) {
			/*if (errno)
				warn("%s", ppc->fname);*/
			status = CHECK_STATUS_ERR(ppc, "arena %u: writing BTT Info backup failed",
				arenap->id);
			goto error;
		}

		if (blk_write_flog(ppc, arenap))
			// return PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return NULL;

		if (blk_write_map(ppc, arenap))
			// return PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return NULL;
	}

	// return PMEMPOOL_CHECK_RESULT_CONSISTENT;
	return NULL;

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return status;
}

struct step {
	struct check_status *(*func)(PMEMpoolcheck *, union location *loc);
	enum pool_type type;
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
		.func		= NULL,

	},
};

/*
 * step -- perform single step according to its parameters
 */
static inline struct check_status *
step(PMEMpoolcheck *ppc, union location *loc)
{
	const struct step *step = &steps[loc->step++];

	struct check_status *status = NULL;
	if (step->type & ppc->pool->params.type)
		status = step->func(ppc, loc);

	return status;
}

/*
 * check_write --
 */
struct check_status *
check_write(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union location) !=
		sizeof (struct check_instep_location));

	union location *loc = (union location *)check_step_location_get(ppc->data);
	struct check_status *status = NULL;

	while (loc->step != CHECK_STEP_COMPLETE &&
		steps[loc->step].func != NULL) {

		status = step(ppc, loc);
		if (status != NULL)
			goto cleanup;
	}
cleanup:
	return status;
}
