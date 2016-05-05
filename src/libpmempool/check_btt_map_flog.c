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
 * check_btt_map_flog.c -- check btt map and flog
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
#include "check_btt_map_flog.h"

union location {
	struct {
		struct arena *arenap;
		uint32_t narena;
		uint8_t *bitmap;
		uint8_t *fbitmap;
		struct list *list_inval;
		struct list *list_flog_inval;
		struct list *list_unmap;

		uint32_t step;
	};
	struct check_instep instep;
};

enum questions {
	Q_REPAIR_MAP,
	Q_REPAIR_FLOG,
};

/*
 * flog_convert2h -- (internal) convert btt_flog to host byte order
 */
static void
flog_convert2h(struct btt_flog *flogp)
{
	flogp->lba = le32toh(flogp->lba);
	flogp->old_map = le32toh(flogp->old_map);
	flogp->new_map = le32toh(flogp->new_map);
	flogp->seq = le32toh(flogp->seq);
}

/*
 * flog_read -- (internal) read and convert flog from file
 */
static int
flog_read(PMEMpoolcheck *ppc, struct arena *arenap)
{
	uint64_t flogoff = arenap->offset + arenap->btt_info.flogoff;
	uint64_t flogsize = arenap->btt_info.nfree *
		roundup(2 * sizeof (struct btt_flog), BTT_FLOG_PAIR_ALIGN);
	arenap->flogsize = roundup(flogsize, BTT_ALIGNMENT);

	arenap->flog = malloc(arenap->flogsize);
	if (!arenap->flog) {
		ERR("Cannot allocate memory for FLOG entries");
		goto error_malloc;
	}

	if (pool_read(ppc->pool, arenap->flog, arenap->flogsize, flogoff)) {
		ERR("arena %u: cannot read BTT FLOG", arenap->id);
		goto error_read;
	}

	uint8_t *ptr = arenap->flog;
	uint32_t i;
	for (i = 0; i < arenap->btt_info.nfree; i++) {
		struct btt_flog *flog_alpha = (struct btt_flog *)ptr;
		struct btt_flog *flog_beta = (struct btt_flog *)(ptr +
				sizeof (struct btt_flog));

		flog_convert2h(flog_alpha);
		flog_convert2h(flog_beta);

		ptr += BTT_FLOG_PAIR_ALIGN;
	}

	return 0;

error_read:
	free(arenap->flog);
	arenap->flog = NULL;

error_malloc:
	return -1;
}

/*
 * map_read -- (internal) read and convert map from file
 */
static int
map_read(PMEMpoolcheck *ppc, struct arena *arenap)
{
	uint64_t mapoff = arenap->offset + arenap->btt_info.mapoff;
	arenap->mapsize = roundup(arenap->btt_info.external_nlba *
		BTT_MAP_ENTRY_SIZE, BTT_ALIGNMENT);

	arenap->map = malloc(arenap->mapsize);
	if (!arenap->map) {
		ERR("Cannot allocate memory for BTT map");
		goto error_malloc;
	}

	if (pool_read(ppc->pool, arenap->map, arenap->mapsize, mapoff)) {
		ERR("arena %u: cannot read BTT map", arenap->id);
		goto error_read;
	}

	uint32_t i;
	for (i = 0; i < arenap->btt_info.external_nlba; i++)
		arenap->map[i] = le32toh(arenap->map[i]);

	return 0;

error_read:
	free(arenap->map);
	arenap->map = NULL;
error_malloc:
	return -1;
}

/*
 * list_item -- item for simple list
 */
struct list_item {
	LIST_ENTRY(list_item) next;
	uint32_t val;
};

/*
 * list -- simple list for storing numbers
 */
struct list {
	LIST_HEAD(listhead, list_item) head;
	uint32_t count;
};

/*
 * list_alloc -- (internal) allocate an empty list
 */
static struct list *
list_alloc(void)
{
	struct list *list = malloc(sizeof (struct list));
	if (!list) {
		ERR("Cannot allocate memory for list");
		return NULL;
	}
	LIST_INIT(&list->head);
	list->count = 0;
	return list;
}

/*
 * list_push -- (internal) insert new element to the list
 */
static struct list_item *
list_push(struct list *list, uint32_t val)
{
	struct list_item *item = malloc(sizeof (*item));
	if (!item) {
		ERR("Cannot allocate memory for list item");
		return NULL;
	}
	item->val = val;
	list->count++;
	LIST_INSERT_HEAD(&list->head, item, next);
	return item;
}

/*
 * list_pop -- (internal) pop element from list head
 */
static int
list_pop(struct list *list, uint32_t *valp)
{
	if (!LIST_EMPTY(&list->head)) {
		struct list_item *i = LIST_FIRST(&list->head);
		LIST_REMOVE(i, next);
		if (valp)
			*valp = i->val;
		free(i);

		list->count--;

		return 1;
	}
	return 0;
}

/*
 * list_free -- (internal) free the list
 */
static void
list_free(struct list *list)
{
	while (list_pop(list, NULL));
	free(list);
}

/*
 * flog_seq_check -- (internal) check FLOG sequence number value
 */
static int
flog_seq_check(uint32_t seq)
{
	return seq == 0 || seq == 1 || seq == 2 || seq == 3;
}

static const unsigned Nseq[] = { 0, 2, 3, 1 };
#define	NSEQ(seq) (Nseq[(seq) & 3])

/*
 * flog_get_valid -- (internal) return valid flog entry
 */
static struct btt_flog *
flog_get_valid(struct btt_flog *flog_alpha, struct btt_flog *flog_beta)
{
	/*
	 * The interesting cases are:
	 * - no valid seq numbers:  layout consistency error
	 * - one valid seq number:  that's the current entry
	 * - two valid seq numbers: higher number is current entry
	 * - identical seq numbers: layout consistency error
	 */
	if (!flog_seq_check(flog_alpha->seq))
		return NULL;
	if (!flog_seq_check(flog_beta->seq))
		return NULL;
	if (flog_alpha->seq == flog_beta->seq)
		return NULL;

	if (flog_alpha->seq == 0)
		return flog_beta;
	if (flog_beta->seq == 0)
		return flog_alpha;

	if (NSEQ(flog_alpha->seq) == flog_beta->seq)
		return flog_beta;

	return flog_alpha;
}

/*
 * cleanup -- (internal) prepare resources for map and flog check
 */
static int
cleanup(PMEMpoolcheck *ppc, union location *loc)
{
	if (loc->list_unmap)
		list_free(loc->list_unmap);
	if (loc->list_flog_inval)
		list_free(loc->list_flog_inval);
	if (loc->list_inval)
		list_free(loc->list_inval);
	if (loc->fbitmap)
		free(loc->fbitmap);
	if (loc->bitmap)
		free(loc->bitmap);

	return 0;
}

/*
 * prepare -- (internal) prepare resources for map and flog check
 */
static int
prepare(PMEMpoolcheck *ppc, union location *loc)
{
	struct arena *arenap = loc->arenap;

	/* read flog and map entries */
	if (flog_read(ppc, arenap)) {
		CHECK_ERR(ppc, "Cannot read flog");
		goto error;
	}

	if (map_read(ppc, arenap)) {
		CHECK_ERR(ppc, "Cannot read map");
		goto error;
	}

	/* create bitmaps for checking duplicated blocks */
	uint32_t bitmapsize = howmany(arenap->btt_info.internal_nlba, 8);
	loc->bitmap = calloc(bitmapsize, 1);
	if (!loc->bitmap) {
		CHECK_ERR(ppc, "Cannot allocate memory for blocks bitmap");
		goto error;
	}

	loc->fbitmap = calloc(bitmapsize, 1);
	if (!loc->fbitmap) {
		CHECK_ERR(ppc, "Cannot allocate memory for flog bitmap");
		goto error;
	}

	/* list of invalid map entries */
	loc->list_inval = list_alloc();
	if (!loc->list_inval) {
		CHECK_ERR(ppc,
			"Cannot allocate memory for invalid map entries list");
		goto error;
	}

	/* list of invalid flog entries */
	loc->list_flog_inval = list_alloc();
	if (!loc->list_flog_inval) {
		CHECK_ERR(ppc,
			"Cannot allocate memory for invalid flog entries list");
		goto error;
	}

	/* list of unmapped blocks */
	loc->list_unmap = list_alloc();
	if (!loc->list_unmap) {
		CHECK_ERR(ppc,
			"Cannot allocate memory for unmaped blocks list");
		goto error;
	}

	return 0;

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
	cleanup(ppc, loc);
	return -1;
}

/*
 * map_entry_check -- (internal) check single map entry
 */
static int
map_entry_check(PMEMpoolcheck *ppc, union location *loc, uint32_t i)
{
	struct arena *arenap = loc->arenap;
	uint32_t entry = arenap->map[i];
	uint32_t flags = entry & ~BTT_MAP_ENTRY_LBA_MASK;
	entry &= BTT_MAP_ENTRY_LBA_MASK;
	int flags_valid = 1;

	if (ppc->pool->params.is_btt_dev) {
		flags_valid = (flags != BTT_DEV_MAP_ENTRY_INVALID);
	}
	if (flags == 0)
		entry = i;

	/* add duplicated and invalid entries to list */
	if (entry < arenap->btt_info.internal_nlba && flags_valid) {
		if (util_isset(loc->bitmap, entry)) {
			CHECK_INFO(ppc, "arena %u: map entry %u duplicated at "
				"%u", arenap->id, entry, i);
			if (!list_push(loc->list_inval, i))
				return 1;
		} else {
			util_setbit(loc->bitmap, entry);
		}
	} else {
		CHECK_INFO(ppc, "arena %u: invalid map entry at %u", arenap->id,
			i);
		if (!list_push(loc->list_inval, i))
			return 1;
	}

	return 0;
}

/*
 * flog_entry_check -- (internal) check single flog entry
 */
static int
flog_entry_check(PMEMpoolcheck *ppc, union location *loc, uint32_t i,
	uint8_t **ptr)
{
	struct arena *arenap = loc->arenap;

	/* first and second copy of flog entry */
	struct btt_flog *flog_alpha = (struct btt_flog *)*ptr;
	struct btt_flog *flog_beta = (struct btt_flog *)(*ptr +
		sizeof (struct btt_flog));

	/*
	 * Check flog entry and return current one by checking sequence number.
	 */
	struct btt_flog *flog_cur = flog_get_valid(flog_alpha, flog_beta);

	/* insert invalid and duplicated indexes to list */
	if (!flog_cur) {
		CHECK_INFO(ppc, "arena %u: invalid flog entry at %u",
			arenap->id, i);
		if (!list_push(loc->list_flog_inval, i))
			return 1;

		goto next;
	}

	uint32_t entry = flog_cur->old_map & BTT_MAP_ENTRY_LBA_MASK;
	uint32_t new_entry = flog_cur->new_map & BTT_MAP_ENTRY_LBA_MASK;

	/*
	 * Check if lba is in extranal_nlba range, and check if both old_map and
	 * new_map are in internal_nlba range.
	 */
	if (flog_cur->lba >= arenap->btt_info.external_nlba ||
		entry >= arenap->btt_info.internal_nlba ||
		new_entry >= arenap->btt_info.internal_nlba) {
		CHECK_INFO(ppc, "arena %u: invalid flog entry at %u",
			arenap->id, entry);
		if (!list_push(loc->list_flog_inval, i))
			return 1;
	}

	if (util_isset(loc->fbitmap, entry)) {
		/*
		 * Here we have two flog entries which holds the same free block
		 */
		CHECK_INFO(ppc, "arena %u: duplicated flog entry at %u\n",
			arenap->id, entry);
		if (!list_push(loc->list_flog_inval, i))
			return 1;
	} else if (util_isset(loc->bitmap, entry)) {
		/* here we have probably an unfinished write */
		if (util_isset(loc->bitmap, new_entry)) {
			/* Both old_map and new_map are already used in map. */
			CHECK_INFO(ppc, "arena %u: duplicated flog entry at "
				"%u\n", arenap->id, entry);
			if (!list_push(loc->list_flog_inval, i))
				return 1;
		} else {
			/*
			 * Unfinished write. Next time pool is opened, the map
			 * will be updated to new_map.
			 */
			util_setbit(loc->bitmap, new_entry);
			util_setbit(loc->fbitmap, entry);
		}
	} else {
		int flog_valid = 1;
		/*
		 * Either flog entry is in its initial state:
		 * - current_btt_flog entry is first one in pair
		 * - current_btt_flog.lba == i (index of pair in flog)
		 * - current_btt_flog.old_map == current_btt_flog.new_map
		 * - current_btt_flog.old_map == external_nlba + i
		 * - current_btt_flog.seq == 0b01
		 * - second flog entry in pair is zeroed
		 * or
		 * btt_map[current_btt_flog.lba] == current_btt_flog.new_map
		 */
		if (entry == new_entry)
			flog_valid = (flog_cur == flog_alpha) &&
				(flog_cur->lba == i) &&
				(flog_cur->seq == 1) &&
				(entry == loc->arenap->btt_info.external_nlba
				+ i) &&
				(!check_memory((const uint8_t *)flog_beta,
				sizeof (*flog_beta), 0));
		else
			flog_valid = (loc->arenap->map[flog_cur->lba] &
				BTT_MAP_ENTRY_LBA_MASK) == new_entry;

		if (flog_valid) {
			/* totally fine case */
			util_setbit(loc->bitmap, entry);
			util_setbit(loc->fbitmap, entry);
		} else {
			CHECK_INFO(ppc, "arena %u: invalid flog entry at %u",
				arenap->id, entry);
			if (!list_push(loc->list_flog_inval, i))
				return 1;
		}
	}

next:
	*ptr += BTT_FLOG_PAIR_ALIGN;
	return 0;
}

/*
 * arena_map_flog_check -- (internal) check map and flog
 */
static int
arena_map_flog_check(PMEMpoolcheck *ppc, union location *loc)
{
	struct arena *arenap = loc->arenap;

	/* check map entries */
	uint32_t i;
	for (i = 0; i < arenap->btt_info.external_nlba; i++) {
		if (map_entry_check(ppc, loc, i))
			goto error_push;
	}

	/* check flog entries */
	uint8_t *ptr = arenap->flog;
	for (i = 0; i < arenap->btt_info.nfree; i++) {
		if (flog_entry_check(ppc, loc, i, &ptr))
			goto error_push;
	}

	/* check unmapped blocks and insert to list */
	for (i = 0; i < arenap->btt_info.internal_nlba; i++) {
		if (!util_isset(loc->bitmap, i)) {
			CHECK_INFO(ppc, "arena %u: unmapped block %u",
				arenap->id, i);
			if (!list_push(loc->list_unmap, i))
				goto error_push;
		}
	}

	if (loc->list_unmap->count)
		CHECK_INFO(ppc, "arena %u: number of unmapped blocks: %u",
			arenap->id, loc->list_unmap->count);
	if (loc->list_inval->count)
		CHECK_INFO(ppc, "arena %u: number of invalid map entries: %u",
			arenap->id, loc->list_inval->count);
	if (loc->list_flog_inval->count)
		CHECK_INFO(ppc, "arena %u: number of invalid flog entries: %u",
			arenap->id, loc->list_flog_inval->count);

	if (!ppc->args.repair && loc->list_unmap->count > 0) {
		ppc->result = PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT;
		return 0;
	}

	/*
	 * We are able to repair if and only if number of unmapped blocks is
	 * equal to sum of invalid map and flog entries.
	 */
	if (loc->list_unmap->count != (loc->list_inval->count +
		loc->list_flog_inval->count)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		CHECK_ERR(ppc, "arena %u: cannot repair map and flog",
			arenap->id);
		return -1;
	}

	if (loc->list_inval->count > 0) {
		CHECK_ASK(ppc, Q_REPAIR_MAP, "Do you want repair invalid map "
			"entries?");
	}

	if (loc->list_flog_inval->count > 0) {
		CHECK_ASK(ppc, Q_REPAIR_FLOG, "Do you want to repair invalid "
			"flog entries?");
	}

	return check_questions_sequence_validate(ppc);

error_push:
	CHECK_ERR(ppc, "Cannot allocate momory for list item");
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
	cleanup(ppc, loc);
	return -1;
}

/*
 * arena_map_flog_fix -- (internal) fix map and flog
 */
static int
arena_map_flog_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;

	uint32_t inval;
	uint32_t unmap;
	switch (question) {
	case Q_REPAIR_MAP:
		/*
		 * Repair invalid or duplicated map entries by using unmapped
		 * blocks.
		 */
		while (list_pop(loc->list_inval, &inval)) {
			if (!list_pop(loc->list_unmap, &unmap)) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return -1;
			}
			loc->arenap->map[inval] = unmap | BTT_MAP_ENTRY_ERROR;
			CHECK_INFO(ppc, "arena %u: storing 0x%x at %u entry",
				loc->arenap->id, loc->arenap->map[inval],
				inval);
		}
		break;
	case Q_REPAIR_FLOG:
		/* repair invalid flog entries using unmapped blocks */
		while (list_pop(loc->list_flog_inval, &inval)) {
			if (!list_pop(loc->list_unmap, &unmap)) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return -1;
			}

			struct btt_flog *flog_alpha = (struct btt_flog *)
				(loc->arenap->flog +
				inval * BTT_FLOG_PAIR_ALIGN);
			struct btt_flog *flog_beta = (struct btt_flog *)
				(loc->arenap->flog +
				inval * BTT_FLOG_PAIR_ALIGN +
				sizeof (struct btt_flog));
			memset(flog_beta, 0, sizeof (*flog_beta));
			uint32_t entry = unmap | BTT_MAP_ENTRY_ERROR;
			flog_alpha->lba = inval;
			flog_alpha->new_map = entry;
			flog_alpha->old_map = entry;
			flog_alpha->seq = 1;

			CHECK_INFO(ppc, "arena %u: repairing flog at %u with "
				"free block entry 0x%x", loc->arenap->id,
				inval, entry);
		}
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return 0;
}

struct step {
	int (*check)(PMEMpoolcheck *, union location *loc);
	int  (*fix)(PMEMpoolcheck *ppc, struct check_instep *location,
		uint32_t question, void *ctx);
};

static const struct step steps[] = {
	{
		.check	= prepare,

	},
	{
		.check	= arena_map_flog_check,

	},
	{
		.fix	= arena_map_flog_fix,

	},
	{
		.check	= cleanup,

	},
	{
		.check	= NULL,

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

		status = check_answer_loop(ppc, &loc->instep, NULL, step->fix);
	} else
		status = step->check(ppc, loc);

	return status;
}

/*
 * check_btt_map_flog -- perform check and fixing of map and flog
 */
void
check_btt_map_flog(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union location) !=
		sizeof (struct check_instep));

	union location *loc =
		(union location *)check_step_location_get(ppc->data);

	if (ppc->pool->blk_no_layout)
		return;

	if (!loc->arenap && loc->narena == 0 &&
		ppc->result != PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS) {
		CHECK_INFO(ppc, "checking BTT map and flog");
		loc->arenap = TAILQ_FIRST(&ppc->pool->arenas);
		loc->narena = 0;
	}

	while (loc->arenap != NULL) {
		if (ppc->result != PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS &&
			loc->step == 0) {
			CHECK_INFO(ppc, "arena %u: checking map and flog",
				loc->narena);
		}

		while (loc->step != CHECK_STEP_COMPLETE &&
			(steps[loc->step].check != NULL ||
			steps[loc->step].fix != NULL)) {

			if (step(ppc, loc))
				return;
		}
		loc->arenap = TAILQ_NEXT(loc->arenap, next);
		loc->narena++;
		loc->step = 0;
	}
}
