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
 * check_pool_hdr.c -- pool header check
 */

#include <stdint.h>
#include <sys/mman.h>

#include "out.h"
#include "util.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
#include "check_utils.h"
#include "check_pool_hdr.h"

union check_pool_hdr_location {
	struct {
		unsigned replica;
		unsigned part;
		uint32_t step;
	};
	struct check_instep_location instep;
};

enum check_pool_hdr_questions {
	CHECK_POOL_HDR_Q_DEFAULT_SIGNATURE,
	CHECK_POOL_HDR_Q_DEFAULT_MAJOR,
	CHECK_POOL_HDR_Q_DEFAULT_COMPAT_FEATURES,
	CHECK_POOL_HDR_Q_DEFAULT_INCOMPAT_FEATURES,
	CHECK_POOL_HDR_Q_DEFAULT_RO_COMPAT_FEATURES,
	CHECK_POOL_HDR_Q_ZERO_UNUSED_AREA,
	CHECK_POOL_HDR_Q_CRTIME,
	CHECK_POOL_HDR_Q_CHECKSUM,
	CHECK_POOL_HDR_Q_BLK_UUID_FROM_BTT_INFO,
	CHECK_POOL_HDR_Q_UUID_FROM_VALID_PART,
	CHECK_POOL_HDR_Q_REGENERATE_UUIDS,
	CHECK_POOL_HDR_Q_SET_VALID_UUID,
	CHECK_POOL_HDR_Q_SET_NEXT_PART_UUID,
	CHECK_POOL_HDR_Q_SET_PREV_PART_UUID,
	CHECK_POOL_HDR_Q_SET_NEXT_REPL_UUID,
	CHECK_POOL_HDR_Q_SET_PREV_REPL_UUID
};

/*
 * check_pool_hdr_possible_type -- return possible type of pool
 */
static enum pool_type
check_pool_hdr_possible_type(PMEMpoolcheck *ppc)
{
	/*
	 * We can scan pool file for valid BTT Info header
	 * if found it means this is pmem blk pool.
	 */
	if (pool_get_first_valid_arena(ppc->pool->set_file, &ppc->pool->bttc)) {
		return POOL_TYPE_BLK;
	}

	return POOL_TYPE_UNKNOWN;
}

/*
 * check_pool_hdr_valid -- return true if pool header is valid
 */
static int
check_pool_hdr_valid(struct pool_hdr *hdrp)
{
	return check_memory((void *)hdrp, sizeof (*hdrp), 0) &&
		util_checksum(hdrp, sizeof (*hdrp), &hdrp->checksum, 0);
}

/*
 * check_pool_hdr_supported -- check if pool type is supported
 */
static int
check_pool_hdr_supported(enum pool_type type)
{
	switch (type) {
	case POOL_TYPE_LOG:
		return 1;
	case POOL_TYPE_BLK:
		return 1;
	case POOL_TYPE_OBJ:
	default:
		return 0;
	}
}

/*
 * check_pool_hdr_get -- get pool header from given location
 */
static void
check_pool_hdr_get(PMEMpoolcheck *ppc, struct pool_hdr *hdr,
	struct pool_hdr **shdr, union check_pool_hdr_location *loc)
{
	struct pool_replica *rep =
		ppc->pool->set_file->poolset->replica[loc->replica];
	struct pool_hdr *hdrp = rep->part[loc->part].hdr;
	memcpy(hdr, hdrp, sizeof (*hdr));

	if (shdr != NULL)
		*shdr = hdrp;
}

/*
 * check_pool_hdr_checksum -- check pool header by checksum
 */
static struct check_status *
check_pool_hdr_checksum(PMEMpoolcheck *ppc, union check_pool_hdr_location *loc)
{
	LOG(2, "checking pool header\n");
	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);

	int cs_valid = check_pool_hdr_valid(&hdr);

	if (check_memory((void *)&hdr, sizeof (hdr), 0) == 0) {
		if (!ppc->repair) {
			ppc->result = PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT;
			return CHECK_STATUS_ERR(ppc, "empty pool hdr");
		}
	} else {
		if (cs_valid) {
			enum pool_type type = pool_hdr_get_type(&hdr);
			if (type == POOL_TYPE_UNKNOWN) {
				if (!ppc->repair) {
					ppc->result =
					PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT;
					return CHECK_STATUS_ERR(ppc,
						"invalid signature");
				} else {
					LOG(1, "invalid signature");
				}
			} else {
				// valid check sum
				loc->step = CHECK_STEPS_COMPLETE;
				return NULL;
			}
		} else {
			if (!ppc->repair) {
				ppc->result =
					PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT;
				return CHECK_STATUS_ERR(ppc, "incorrect pool "
					"header checksum");
			} else {
				LOG(1, "incorrect pool header checksum");
			}
		}
	}

	ASSERTeq(ppc->repair, true);

	if (ppc->pool->params.type == POOL_TYPE_UNKNOWN) {
		ppc->pool->params.type = check_pool_hdr_possible_type(ppc);
		if (ppc->pool->params.type == POOL_TYPE_UNKNOWN) {
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "cannot determine pool "
				"type");
		}
	}

	if (!check_pool_hdr_supported(ppc->pool->params.type)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
		return CHECK_STATUS_ERR(ppc, "Unsupported pool type '%s'",
			pool_get_type_str(ppc->pool->params.type));
	}

	return NULL;
}

/*
 * check_pool_hdr_default -- check some default values in pool header
 */
static struct check_status *
check_pool_hdr_default(PMEMpoolcheck *ppc, union check_pool_hdr_location *loc)
{
	ASSERTeq(ppc->repair, true);

	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);
	pool_hdr_convert2h(&hdr);

	struct pool_hdr def_hdr;
	pool_hdr_default(ppc->pool->params.type, &def_hdr);

	if (memcmp(hdr.signature, def_hdr.signature, POOL_HDR_SIG_LEN)) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_DEFAULT_SIGNATURE,
			"pool_hdr.signature is not valid. Do you want to set "
			"it to %.8s?", def_hdr.signature);
	}

	if (hdr.major != def_hdr.major) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_DEFAULT_MAJOR,
			"pool_hdr.major is not valid. Do you want to set it "
			"to default value 0x%x?", def_hdr.major);
	}

	if (hdr.compat_features != def_hdr.compat_features) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_DEFAULT_COMPAT_FEATURES,
			"pool_hdr.compat_features is not valid. Do you want "
			"to set it to default value 0x%x?",
			def_hdr.compat_features);
	}

	if (hdr.incompat_features != def_hdr.incompat_features) {
		CHECK_STATUS_ASK(ppc,
			CHECK_POOL_HDR_Q_DEFAULT_INCOMPAT_FEATURES,
			"pool_hdr.incompat_features is not valid. Do you want"
			"to set it to default value 0x%x?",
			def_hdr.incompat_features);
	}

	if (hdr.ro_compat_features != def_hdr.ro_compat_features) {
		CHECK_STATUS_ASK(ppc,
			CHECK_POOL_HDR_Q_DEFAULT_RO_COMPAT_FEATURES,
			"pool_hdr.ro_compat_features is not valid. Do you want"
			"to set it to default value 0x%x?",
			def_hdr.ro_compat_features);
	}

	if (check_memory(hdr.unused, sizeof (hdr.unused), 0)) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_ZERO_UNUSED_AREA,
			"Unused area is not filled by zeros. Do you want to "
			"fill it up?");
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pool_hdr_default_fix -- fix some default values in pool header
 */
static struct check_status *
check_pool_hdr_default_fix(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	struct pool_hdr hdr;
	struct pool_hdr *hdrp;
	check_pool_hdr_get(ppc, &hdr, &hdrp, loc);
	pool_hdr_convert2h(&hdr);

	struct pool_hdr def_hdr;
	pool_hdr_default(ppc->pool->params.type, &def_hdr);

	struct check_status *answer;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_POOL_HDR_Q_DEFAULT_SIGNATURE:
				memcpy(hdr.signature, def_hdr.signature,
					POOL_HDR_SIG_LEN);
				break;
			case CHECK_POOL_HDR_Q_DEFAULT_MAJOR:
				hdr.major = def_hdr.major;
				break;
			case CHECK_POOL_HDR_Q_DEFAULT_COMPAT_FEATURES:
				hdr.compat_features = def_hdr.compat_features;
				break;
			case CHECK_POOL_HDR_Q_DEFAULT_INCOMPAT_FEATURES:
				hdr.incompat_features =
					def_hdr.incompat_features;
				break;
			case CHECK_POOL_HDR_Q_DEFAULT_RO_COMPAT_FEATURES:
				hdr.ro_compat_features =
					def_hdr.ro_compat_features;
				break;
			case CHECK_POOL_HDR_Q_ZERO_UNUSED_AREA:
				memset(hdr.unused, 0, sizeof (hdr.unused));
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	pool_hdr_convert2le(&hdr);
	memcpy(hdrp, &hdr, sizeof (*hdrp));
	msync(hdrp, sizeof (*hdrp), MS_SYNC);
	return NULL;
}

/*
 * check_pool_hdr_get_valid_part -- returns valid part replica and part ids
 */
static int
check_pool_hdr_get_valid_part(PMEMpoolcheck *ppc, unsigned rid, unsigned pid,
	unsigned *ridp, unsigned *pidp)
{
	for (unsigned r = 0; r < ppc->pool->set_file->poolset->nreplicas; r++) {
		struct pool_replica *rep =
			ppc->pool->set_file->poolset->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			if (r == rid && p == pid)
				continue;

			if (check_pool_hdr_valid(rep->part[p].hdr)) {
				*ridp = r;
				*pidp = p;
				return 0;
			}
		}
	}

	return -1;
}

/*
 * check_pool_hdr_poolset_uuid -- check poolset_uuid field
 */
static struct check_status *
check_pool_hdr_poolset_uuid(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);
	pool_hdr_convert2h(&hdr);

	/* for blk pool we can take the UUID from BTT Info header */
	if (ppc->pool->params.type == POOL_TYPE_BLK &&
		ppc->pool->bttc.valid) {
		if (memcmp(hdr.poolset_uuid,
			ppc->pool->bttc.btt_info.parent_uuid,
			POOL_HDR_UUID_LEN) == 0) {
			return NULL;
		}

		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_BLK_UUID_FROM_BTT_INFO,
			"Invalid pool_hdr.poolset_uuid. Do you want to set it "
			"to %s from BTT Info?",
			check_utils_get_uuid_str(
				ppc->pool->bttc.btt_info.parent_uuid));
	} else if (ppc->pool->params.is_poolset) {
		unsigned rid = 0;
		unsigned pid = 0;
		if (check_pool_hdr_get_valid_part(ppc, loc->replica, loc->part,
			&rid, &pid) != 0) {
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "Can not repair "
				"pool_hdr.poolset_uuid");
		}
		struct pool_hdr *valid_hdrp =
			ppc->pool->set_file->poolset->replica[rid]->part[pid].
			hdr;
		if (memcmp(hdr.poolset_uuid, valid_hdrp->poolset_uuid,
					POOL_HDR_UUID_LEN) == 0)
			return NULL;
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_UUID_FROM_VALID_PART,
			"Invalid pool_hdr.poolset_uuid. Do you want to set it "
			"to %s from valid pool file part ?",
			check_utils_get_uuid_str(valid_hdrp->poolset_uuid));
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pool_hdr_poolset_uuid_fix -- fix poolset_uuid field
 */
static struct check_status *
check_pool_hdr_poolset_uuid_fix(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	struct pool_hdr hdr;
	struct pool_hdr *hdrp;
	check_pool_hdr_get(ppc, &hdr, &hdrp, loc);
	pool_hdr_convert2h(&hdr);

	struct check_status *answer;
	unsigned rid = 0;
	unsigned pid = 0;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_POOL_HDR_Q_BLK_UUID_FROM_BTT_INFO:
				LOG(1, "setting pool_hdr.poolset_uuid to %s\n",
					check_utils_get_uuid_str(
					ppc->pool->bttc.btt_info.parent_uuid));
				memcpy(hdr.poolset_uuid,
					ppc->pool->bttc.btt_info.parent_uuid,
					POOL_HDR_UUID_LEN);
				ppc->pool->uuid_op = UUID_FROM_BTT;
				break;
			case CHECK_POOL_HDR_Q_UUID_FROM_VALID_PART:
				if (check_pool_hdr_get_valid_part(ppc,
					loc->replica, loc->part, &rid, &pid)
					!= 0) {
					ppc->result =
					PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
					return CHECK_STATUS_ERR(ppc,
						"Can not repair "
						"pool_hdr.poolset_uuid");
				}
				struct pool_hdr *valid_hdrp =
					ppc->pool->set_file->poolset->
					replica[rid]->part[pid].hdr;
				LOG(1, "setting pool_hdr.poolset_uuid to %s\n",
					check_utils_get_uuid_str(
						valid_hdrp->poolset_uuid));
				memcpy(hdr.poolset_uuid,
					valid_hdrp->poolset_uuid,
					POOL_HDR_UUID_LEN);
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	pool_hdr_convert2le(&hdr);
	memcpy(hdrp, &hdr, sizeof (*hdrp));
	msync(hdrp, sizeof (*hdrp), MS_SYNC);
	return NULL;
}

/*
 * check_pool_hdr_checksum_retry -- check if checksum match after all performed
 *	fixes
 */
static struct check_status *
check_pool_hdr_checksum_retry(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	LOG(2, "checking pool header\n");
	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);

	if (check_pool_hdr_valid(&hdr)) {
		loc->step = CHECK_STEPS_COMPLETE;
	}

	return NULL;
}

/*
 * check_pool_hdr_gen -- generate pool hdr values
 */
static struct check_status *
check_pool_hdr_gen(PMEMpoolcheck *ppc, union check_pool_hdr_location *loc)
{
	LOG(2, "checking pool header\n");
	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);
	pool_hdr_convert2h(&hdr);

	if (hdr.crtime > (uint64_t)ppc->pool->set_file->mtime) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_CRTIME,
			"pool_hdr.crtime is not valid. Do you want to set it "
			"to file's modtime [%s]?",
			check_get_time_str(ppc->pool->set_file->mtime));
	}

	CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_CHECKSUM,
		"Do you want to regenerate checksum?");

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pool_hdr_gen_fix -- fix some pool hdr values by overwrite with
 *	generated values
 */
static struct check_status *
check_pool_hdr_gen_fix(PMEMpoolcheck *ppc, union check_pool_hdr_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	struct pool_hdr hdr;
	struct pool_hdr *hdrp;
	check_pool_hdr_get(ppc, &hdr, &hdrp, loc);

	struct check_status *answer;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_POOL_HDR_Q_CRTIME:
				pool_hdr_convert2h(&hdr);
				hdr.crtime =
					(uint64_t)ppc->pool->set_file->mtime;
				pool_hdr_convert2le(&hdr);
				break;
			case CHECK_POOL_HDR_Q_CHECKSUM:
				util_checksum(&hdr, sizeof (hdr),
					&hdr.checksum, 1);
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	memcpy(hdrp, &hdr, sizeof (*hdrp));
	msync(hdrp, sizeof (*hdrp), MS_SYNC);
	return NULL;
}

/*
 * check_pool_hdr_all_uuid_same -- check if all uuids are same and non-zero
 */
static int
check_pool_hdr_all_uuid_same(unsigned char (*uuids)[POOL_HDR_UUID_LEN], int n)
{
	if (!check_memory(uuids[0], POOL_HDR_UUID_LEN, 0))
		return 0;
	for (int i = 1; i < n; i++) {
		if (memcmp(uuids[0], uuids[i], POOL_HDR_UUID_LEN))
			return 0;
	}

	return 1;
}

/*
 * check_pool_hdr_get_max_same_uuid -- return indices of two the same uuids
 */
static int
check_pool_hdr_get_max_same_uuid(unsigned char (*uuids)[POOL_HDR_UUID_LEN],
		int n, int *indexp)
{
	int max = 0;
	for (int i = 0; i < n; i++) {
		int icount = 0;
		if (!check_memory(uuids[i], POOL_HDR_UUID_LEN, 0))
			continue;
		for (int j = 0; j < n; j++) {
			if (i == j)
				continue;
			if (!memcmp(uuids[i], uuids[j], POOL_HDR_UUID_LEN))
				icount++;
		}

		if (icount > max) {
			max = icount;
			*indexp = i;
		}
	}

	if (max)
		return 0;
	return -1;
}

/*
 * check_pool_hdr_uuids_single -- check UUID values for a single pool file
 */
static struct check_status *
check_pool_hdr_uuids_single(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	unsigned nreplicas = ppc->pool->set_file->poolset->nreplicas;
	unsigned nparts = ppc->pool->set_file->poolset->replica[loc->
		replica]->nparts;
	if (nreplicas != 1 || nparts != 1)
		return NULL;

	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);
	pool_hdr_convert2h(&hdr);

	if (!check_pool_hdr_all_uuid_same(&hdr.uuid, 5)) {
		int index = 0;
		if (check_pool_hdr_get_max_same_uuid(&hdr.uuid, 5, &index)) {
			CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_REGENERATE_UUIDS,
				"UUID values don't match. Do you want to "
				"regenerate UUIDs?");
		} else {
			CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_SET_VALID_UUID,
				"UUID values don't match. Do you want to set "
				"it to valid value?");
		}
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pool_hdr_set_all_uuids -- set all uuids to the specified one
 */
static void
check_pool_hdr_set_all_uuids(unsigned char (*uuids)[POOL_HDR_UUID_LEN],
		int n, int index)
{
	for (int i = 0; i < n; i++) {
		if (i == index)
			continue;
		memcpy(uuids[i], uuids[index], POOL_HDR_UUID_LEN);
	}
}

/*
 * check_pool_hdr_uuids_single_fix -- fix UUID values for a single pool file
 */
static struct check_status *
check_pool_hdr_uuids_single_fix(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	unsigned nreplicas = ppc->pool->set_file->poolset->nreplicas;
	unsigned nparts =
		ppc->pool->set_file->poolset->replica[loc->replica]->nparts;
	if (nreplicas != 1 || nparts != 1)
		return NULL;

	struct pool_hdr hdr;
	struct pool_hdr *hdrp;
	check_pool_hdr_get(ppc, &hdr, &hdrp, loc);
	pool_hdr_convert2h(&hdr);

	struct check_status *answer;
	int index = 0;
	int ret;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_POOL_HDR_Q_REGENERATE_UUIDS:
				ret = util_uuid_generate(hdr.uuid);
				if (ret < 0) {
					ppc->result =
					PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
					return CHECK_STATUS_ERR(ppc, "uuid "
						"generation failed");
				}
				LOG(1, "setting UUIDs to: %s\n",
					check_utils_get_uuid_str(
					hdrp->uuid));
				check_pool_hdr_set_all_uuids(&hdr.uuid, 5, 0);
				break;
			case CHECK_POOL_HDR_Q_SET_VALID_UUID:
				check_pool_hdr_get_max_same_uuid(&hdr.uuid, 5,
					&index);
				unsigned char (*uuid_i)[POOL_HDR_UUID_LEN] =
					&hdr.uuid;
				LOG(2, "setting UUIDs to %s\n",
					check_utils_get_uuid_str(
					uuid_i[index]));
				check_pool_hdr_set_all_uuids(&hdrp->uuid, 5,
					index);
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	pool_hdr_convert2le(&hdr);
	memcpy(hdrp, &hdr, sizeof (*hdrp));
	msync(hdrp, sizeof (*hdrp), MS_SYNC);
	return NULL;
}

/*
 * check_pool_hdr_uuids -- check UUID values for pool file
 */
static struct check_status *
check_pool_hdr_uuids(PMEMpoolcheck *ppc, union check_pool_hdr_location *loc)
{
	unsigned nreplicas = ppc->pool->set_file->poolset->nreplicas;
	unsigned nparts =
		ppc->pool->set_file->poolset->replica[loc->replica]->nparts;
	if (nreplicas == 1 && nparts == 1)
		return NULL;

	unsigned nr = (loc->replica + 1) % nreplicas;
	unsigned pr = (loc->replica - 1) % nreplicas;
	unsigned np = (loc->part + 1) % nparts;
	unsigned pp = (loc->part - 1) % nparts;

	int single_repl = nr == loc->replica && pr == loc->replica;
	int single_part = np == loc->part && pp == loc->part;

	struct pool_replica *rep =
		ppc->pool->set_file->poolset->replica[loc->replica];
	struct pool_replica *next_rep =
		ppc->pool->set_file->poolset->replica[nr];
	struct pool_replica *prev_rep =
		ppc->pool->set_file->poolset->replica[pr];

	struct pool_hdr *next_part_hdrp = rep->part[np].hdr;
	struct pool_hdr *prev_part_hdrp = rep->part[pp].hdr;

	struct pool_hdr *next_repl_hdrp = next_rep->part[0].hdr;
	struct pool_hdr *prev_repl_hdrp = prev_rep->part[0].hdr;

	int next_part_cs_valid = check_pool_hdr_valid(next_part_hdrp);
	int prev_part_cs_valid = check_pool_hdr_valid(prev_part_hdrp);
	int next_repl_cs_valid = check_pool_hdr_valid(next_repl_hdrp);
	int prev_repl_cs_valid = check_pool_hdr_valid(prev_repl_hdrp);

	struct pool_hdr hdr;
	check_pool_hdr_get(ppc, &hdr, NULL, loc);
	pool_hdr_convert2h(&hdr);

	int next_part_valid = !memcmp(hdr.next_part_uuid,
			next_part_hdrp->uuid, POOL_HDR_UUID_LEN);
	int prev_part_valid = !memcmp(hdr.prev_part_uuid,
			prev_part_hdrp->uuid, POOL_HDR_UUID_LEN);

	int next_repl_valid = !memcmp(hdr.next_repl_uuid,
			next_repl_hdrp->uuid, POOL_HDR_UUID_LEN);
	int prev_repl_valid = !memcmp(hdr.prev_repl_uuid,
			prev_repl_hdrp->uuid, POOL_HDR_UUID_LEN);

	if ((single_part || next_part_cs_valid) && !next_part_valid) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_SET_NEXT_PART_UUID,
			"Invalid pool_hdr.next_part_uuid. Do you want to set "
			"it to valid value?");
	}

	if ((single_part || prev_part_cs_valid) && !prev_part_valid) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_SET_PREV_PART_UUID,
			"Invalid pool_hdr.prev_part_uuid. Do you want to set "
			"it to valid value?");
	}

	if ((single_repl || prev_repl_cs_valid) && !next_repl_valid) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_SET_NEXT_REPL_UUID,
			"Invalid pool_hdr.next_repl_uuid. Do you want to set "
			"it to valid value?");
	}

	if ((single_repl || next_repl_cs_valid) && !prev_repl_valid) {
		CHECK_STATUS_ASK(ppc, CHECK_POOL_HDR_Q_SET_PREV_REPL_UUID,
			"Invalid pool_hdr.prev_repl_uuid. Do you want to set "
			"it to valid value?");
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pool_hdr_uuids_fix -- fix UUID values for pool file
 */
static struct check_status *
check_pool_hdr_uuids_fix(PMEMpoolcheck *ppc,
	union check_pool_hdr_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	unsigned nreplicas = ppc->pool->set_file->poolset->nreplicas;
	unsigned nparts =
		ppc->pool->set_file->poolset->replica[loc->replica]->nparts;
	if (nreplicas == 1 && nparts == 1)
		return NULL;

	unsigned nr = (loc->replica + 1) % nreplicas;
	unsigned pr = (loc->replica - 1) % nreplicas;
	unsigned np = (loc->part + 1) % nparts;
	unsigned pp = (loc->part - 1) % nparts;

	struct pool_replica *rep =
		ppc->pool->set_file->poolset->replica[loc->replica];
	struct pool_replica *next_rep =
		ppc->pool->set_file->poolset->replica[nr];
	struct pool_replica *prev_rep =
		ppc->pool->set_file->poolset->replica[pr];

	struct pool_hdr *next_part_hdrp = rep->part[np].hdr;
	struct pool_hdr *prev_part_hdrp = rep->part[pp].hdr;

	struct pool_hdr *next_repl_hdrp = next_rep->part[0].hdr;
	struct pool_hdr *prev_repl_hdrp = prev_rep->part[0].hdr;

	struct pool_hdr hdr;
	struct pool_hdr *hdrp;
	check_pool_hdr_get(ppc, &hdr, &hdrp, loc);

	struct check_status *answer;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_POOL_HDR_Q_SET_NEXT_PART_UUID:
				LOG(2, "setting pool_hdr.next_part_uuid to "
					"%s\n", check_utils_get_uuid_str(
					next_part_hdrp->uuid));
				memcpy(hdr.next_part_uuid,
					next_part_hdrp->uuid,
					POOL_HDR_UUID_LEN);
				break;
			case CHECK_POOL_HDR_Q_SET_PREV_PART_UUID:
				LOG(2, "setting pool_hdr.prev_part_uuid to "
					"%s\n", check_utils_get_uuid_str(
					prev_part_hdrp->uuid));
				memcpy(hdr.prev_part_uuid,
					prev_part_hdrp->uuid,
					POOL_HDR_UUID_LEN);
				break;
			case CHECK_POOL_HDR_Q_SET_NEXT_REPL_UUID:
				LOG(2, "setting pool_hdr.next_repl_uuid to "
					"%s\n", check_utils_get_uuid_str(
					next_repl_hdrp->uuid));
				memcpy(hdr.next_repl_uuid, next_repl_hdrp->uuid,
					POOL_HDR_UUID_LEN);
				break;
			case CHECK_POOL_HDR_Q_SET_PREV_REPL_UUID:
				LOG(2, "setting pool_hdr.prev_repl_uuid to "
					"%s\n", check_utils_get_uuid_str(
					prev_repl_hdrp->uuid));
				memcpy(hdr.prev_repl_uuid, prev_repl_hdrp->uuid,
					POOL_HDR_UUID_LEN);
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	pool_hdr_convert2le(&hdr);
	memcpy(hdrp, &hdr, sizeof (*hdrp));
	msync(hdrp, sizeof (*hdrp), MS_SYNC);
	return NULL;
}

struct check_pool_hdr_step {
	struct check_status *(*func)(PMEMpoolcheck *,
		union check_pool_hdr_location *loc);
};

static const struct check_pool_hdr_step check_pool_hdr_steps[] = {
	{
		.func	= check_pool_hdr_checksum,
	},
	{
		.func	= check_pool_hdr_default,
	},
	{
		.func	= check_pool_hdr_default_fix,
	},
	{
		.func	= check_pool_hdr_poolset_uuid,
	},
	{
		.func	= check_pool_hdr_poolset_uuid_fix,
	},
	{
		.func	= check_pool_hdr_uuids_single,
	},
	{
		.func	= check_pool_hdr_uuids_single_fix,
	},
	{
		.func	= check_pool_hdr_uuids,
	},
	{
		.func	= check_pool_hdr_uuids_fix,
	},
	{
		.func	= check_pool_hdr_checksum_retry,
	},
	{
		.func	= check_pool_hdr_gen,
	},
	{
		.func	= check_pool_hdr_gen_fix,
	},
	{
		.func	= NULL,
	},
};

/*
 * check_pool_hdr -- entry point for pool header checks
 */
struct check_status *
check_pool_hdr(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union check_pool_hdr_location) !=
		sizeof (struct check_instep_location));

	int rdonly = !ppc->repair || ppc->dry_run;
	if (pool_set_file_map_headers(ppc->pool->set_file, rdonly,
			sizeof (struct pool_hdr))) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return CHECK_STATUS_ERR(ppc, "cannot map pool headers");
	}

	union check_pool_hdr_location *loc =
		(union check_pool_hdr_location *)&ppc->data->instep_location;
	unsigned nreplicas = ppc->pool->set_file->poolset->nreplicas;
	struct check_status *status = NULL;

	for (; loc->replica < nreplicas; loc->replica++) {
		struct pool_replica *rep =
			ppc->pool->set_file->poolset->replica[loc->replica];
		for (; loc->part < rep->nparts; loc->part++) {
			if (ppc->result !=
				PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS)
				loc->step = 0;

			while (loc->step != CHECK_STEPS_COMPLETE &&
				check_pool_hdr_steps[loc->step].func != NULL) {
				const struct check_pool_hdr_step *step =
					&check_pool_hdr_steps[loc->step++];
				status = step->func(ppc, loc);
				if (status != NULL)
					goto cleanup_return;
			}
		}
	}

	memcpy(&ppc->pool->hdr.pool,
		ppc->pool->set_file->poolset->replica[0]->part[0].hdr,
		sizeof (struct pool_hdr));
cleanup_return:
	pool_set_file_unmap_headers(ppc->pool->set_file);
	return status;
}
