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

#include "out.h"
#include "btt.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"
#include "check_btt_info.h"

union location {
	struct {
		uint64_t offset;
		uint64_t offset2;
		uint64_t nextoff;
		struct arena *arena;
		uint32_t step;
	};
	struct check_instep instep;
};

enum question {
	Q_RESTORE_FROM_BACKUP,
	Q_REGENERATE,
	Q_REGENERATE_CHECKSUM,
};

struct btt_context {
	PMEMpoolcheck *ppc;
	uint64_t base_off;
	uint64_t len;
};

/*
 * location_release -- (internal) release check_btt_info_loc allocations
 */
static void
location_release(union location *loc)
{
	free(loc->arena);
	loc->arena = NULL;
}

/*
 * btt_info_checksum -- (internal) check BTT Info checksum
 */
static int
btt_info_checksum(PMEMpoolcheck *ppc, union location *loc)
{
	int status = 0;
	loc->arena = calloc(1, sizeof (struct arena));
	if (!loc->arena) {
		ERR("!calloc");
		ppc->result = PMEMPOOL_CHECK_RESULT_INTERNAL_ERROR;
		status = CHECK_ERR(ppc, "cannot allocate memory for arena");
		goto cleanup;
	}

	/* read the BTT Info header at well known offset */
	if (pool_read(ppc->pool, &loc->arena->btt_info,
		sizeof (loc->arena->btt_info), loc->offset)) {
		status = CHECK_ERR(ppc,
			"arena %u: cannot read BTT Info header",
			loc->arena->id);
		goto error;
	}

	loc->arena->id = ppc->pool->narenas;

	/* BLK is consistent even without BTT Layout */
	if (!ppc->pool->params.is_btt_dev && check_memory((const uint8_t *)
		&loc->arena->btt_info, sizeof (loc->arena->btt_info), 0) == 0) {
		CHECK_INFO(ppc, "BTT Layout not written");
		ppc->pool->blk_no_layout = 1;
		loc->step = CHECK_STEP_COMPLETE;
		goto cleanup;
	}

	/* check consistency of BTT Info */
	if (pool_btt_info_valid(&loc->arena->btt_info)) {
		CHECK_INFO(ppc, "arena %u: BTT Info header checksum correct",
			loc->arena->id);
		loc->step = CHECK_STEP_COMPLETE;
	} else {
		if (!ppc->args.repair) {
			status = CHECK_ERR(ppc,
				"arena %u: BTT Info header checksum incorrect",
				loc->arena->id);
			goto error;
		}
	}

	return 0;

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
cleanup:
	location_release(loc);
	return status;
}

/*
 * btt_info_backup -- (internal) check BTT Info backup
 */
static int
btt_info_backup(PMEMpoolcheck *ppc, union location *loc)
{
	ASSERT(ppc->args.repair);
	int status = 0;

	/* BTT Info header is not consistent, so try get BTT Info backup */
	const size_t btt_info_size = sizeof (ppc->pool->bttc.btt_info);
	loc->offset2 = pool_next_arena_offset(ppc, loc->offset) - btt_info_size;

	if (pool_read(ppc->pool, &ppc->pool->bttc.btt_info, btt_info_size,
		loc->offset2) != 0) {
		status = CHECK_ERR(ppc, "arena %u: cannot read BTT Info backup",
			loc->arena->id);
		goto error;
	}

	/* Check whether this BTT Info backup is valid */
	if (pool_btt_info_valid(&ppc->pool->bttc.btt_info)) {
		/* Here we have valid BTT Info backup so we can restore it. */
		CHECK_ASK(ppc, Q_RESTORE_FROM_BACKUP,
			"arena %u: BTT Info header checksum incorrect.|"
			"Restore BTT Info from backup?", loc->arena->id);
	}

	return check_questions_sequence_validate(ppc);

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
	location_release(loc);
	return status;
}

/*
 * btt_info_backup_fix -- (internal) fix BTT Info using its backup
 */
static int
btt_info_backup_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;

	switch (question) {
	case Q_RESTORE_FROM_BACKUP:
		CHECK_INFO(ppc,
			"arena %u: restoring BTT Info header from backup",
			loc->arena->id);

		memcpy(&loc->arena->btt_info, &ppc->pool->bttc.btt_info,
			sizeof (loc->arena->btt_info));
		loc->step = CHECK_STEP_COMPLETE;
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return 0;
}

/*
 * btt_info_gen -- (internal) ask whether try to regenerate BTT Info
 */
static int
btt_info_gen(PMEMpoolcheck *ppc, union location *loc)
{
	CHECK_ASK(ppc, Q_REGENERATE,
		"arena %u: BTT Info header checksum incorrect.|Do you want to "
		"regenerate BTT Info?", loc->arena->id);

	return check_questions_sequence_validate(ppc);
}

/*
 * btt_info_gen_fix -- (internal) fix by regenerating BTT Info
 */
static int
btt_info_gen_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;

	switch (question) {
	case Q_REGENERATE:
		CHECK_INFO(ppc, "arena %u: regenerating BTT Info header",
			loc->arena->id);

		/*
		 * We do not have valid BTT Info backup so we get first valid
		 * BTT Info and try to calculate BTT Info for current arena
		 */
		loc->offset2 = pool_get_first_valid_btt(ppc,
			&ppc->pool->bttc.btt_info, loc->offset + BTT_MAX_ARENA);

		if (loc->offset2 == 0) {
			/* Without valid BTT Info we can not proceed */
			CHECK_INFO(ppc, "Can not find any valid BTT Info");
			return -1;
		}

		uint64_t arena_size = ppc->pool->set_file->size - loc->offset;
		if (arena_size > BTT_MAX_ARENA)
			arena_size = BTT_MAX_ARENA;

		uint64_t space_left = ppc->pool->set_file->size - loc->offset -
			arena_size;

		struct btt_info *bttd = &loc->arena->btt_info;
		struct btt_info *btts = &ppc->pool->bttc.btt_info;

		/*
		 * All valid BTT Info structures have the same signature, UUID,
		 * parent UUID, flags, major, minor, external LBA size, internal
		 * LBA size, nfree, info size and data offset
		 */
		memcpy(bttd->sig, btts->sig, BTTINFO_SIG_LEN);
		memcpy(bttd->uuid, btts->uuid, BTTINFO_UUID_LEN);
		memcpy(bttd->parent_uuid, btts->parent_uuid, BTTINFO_UUID_LEN);
		bttd->flags = btts->flags;
		bttd->major = btts->major;
		bttd->minor = btts->minor;

		/* Other parameters can be calculated */
		if (btt_info_set(bttd, btts->external_lbasize, btts->nfree,
			arena_size, space_left)) {
			CHECK_ERR(ppc, "Can not restore BTT Info");
			return -1;
		}

		ASSERTeq(bttd->external_lbasize, btts->external_lbasize);
		ASSERTeq(bttd->internal_lbasize, btts->internal_lbasize);
		ASSERTeq(bttd->nfree, btts->nfree);
		ASSERTeq(bttd->infosize, btts->infosize);
		ASSERTeq(bttd->dataoff, btts->dataoff);

		memset(bttd->unused, 0, BTTINFO_UNUSED_LEN);
		return 0;

	default:
		ERR("not implemented question id: %u", question);
		return -1;
	}
}

/*
 * btt_info_checksum_retry -- (internal) check BTT Info checksum
 */
static int
btt_info_checksum_retry(PMEMpoolcheck *ppc, union location *loc)
{
	int status = 0;

	pool_btt_info_convert2le(&loc->arena->btt_info);

	/* check consistency of BTT Info */
	if (pool_btt_info_valid(&loc->arena->btt_info)) {
		CHECK_INFO(ppc, "arena %u: BTT Info header checksum correct",
			loc->arena->id);
		loc->step = CHECK_STEP_COMPLETE;
		/* futher steps require BTT Info in host byte order */
		pool_btt_info_convert2h(&loc->arena->btt_info);
		return 0;
	}

	if (!ppc->args.advanced) {
		status = CHECK_ERR(ppc,
			"arena %u: BTT Info header checksum incorrect",
			loc->arena->id);
		goto error;
	}

	CHECK_ASK(ppc, Q_REGENERATE_CHECKSUM,
		"arena %u: BTT Info header checksum incorrect.|Do you want to "
		"regenerate BTT Info checksum?", loc->arena->id);

	return check_questions_sequence_validate(ppc);

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
	location_release(loc);
	return status;
}

/*
 * btt_info_checksum_fix -- (internal) fix by regenerating BTT Info checksum
 */
static int
btt_info_checksum_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;

	switch (question) {
	case Q_REGENERATE_CHECKSUM:
		util_checksum(&loc->arena->btt_info, sizeof (struct btt_info),
			&loc->arena->btt_info.checksum, 1);
		return 0;

	default:
		ERR("not implemented question id: %u", question);
		return -1;
	}
}

struct step {
	int (*check)(PMEMpoolcheck *, union location *loc);
	int (*fix)(PMEMpoolcheck *ppc, struct check_instep *location,
		uint32_t question, void *ctx);
};

static const struct step steps[] = {
	{
		.check		= btt_info_checksum,

	},
	{
		.check		= btt_info_backup,
	},
	{
		.fix		= btt_info_backup_fix,
	},
	{
		.check		= btt_info_gen,
	},
	{
		.fix		= btt_info_gen_fix,
	},
	{
		.check		= btt_info_checksum_retry,
	},
	{
		.fix		= btt_info_checksum_fix,
	},
	{
		.check		= NULL,
	},
};

/*
 * step -- (internal) perform single step according to its parameters
 */
static inline int
step(PMEMpoolcheck *ppc, union location *loc)
{
	const struct step *step = &steps[loc->step++];

	int status = 0;
	if (step->fix != NULL) {
		if (!check_has_answer(ppc->data))
			return 0;

		status = check_answer_loop(ppc, (struct check_instep *)loc,
			NULL, step->fix);

		if (check_has_error(ppc->data)) {
			struct check_status *err = check_pop_error(ppc->data);
			check_status_release(ppc, err);
			status = CHECK_ERR(ppc,
				"arena %u: cannot repair BTT Info header",
				loc->arena->id);
			location_release(loc);
		}
	} else
		status = step->check(ppc, loc);

	return status;
}

/*
 * check_btt_info -- entry point for btt info check
 */
void
check_btt_info(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union location) !=
		sizeof (struct check_instep));

	union location *loc =
		(union location *)check_step_location_get(ppc->data);

	if (!loc->offset) {
		CHECK_INFO(ppc, "checking BTT Info headers");
		loc->offset = BTT_ALIGNMENT;
		if (!ppc->pool->params.is_btt_dev)
			loc->offset += BTT_ALIGNMENT;
		loc->nextoff = 0;
	}

	do {
		if (ppc->result != PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS) {
			loc->offset += loc->nextoff;
			loc->offset2 = 0;
			loc->nextoff = 0;
			loc->step = 0;
		}

		while (loc->step != CHECK_STEP_COMPLETE &&
			(steps[loc->step].check != NULL ||
			steps[loc->step].fix != NULL)) {

			if (step(ppc, loc) || ppc->pool->blk_no_layout == 1)
				return;
		}

		/* save offset and insert BTT to cache for next steps */
		loc->arena->offset = loc->offset;
		loc->arena->valid = true;
		check_insert_arena(ppc, loc->arena);
		loc->nextoff = le64toh(loc->arena->btt_info.nextoff);
	} while (loc->nextoff > 0);
}
