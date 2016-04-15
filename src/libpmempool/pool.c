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
 * pool.c -- pool processing functions
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "out.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "libpmempool.h"
#include "pool.h"
#include "obj.h"
#include "libpmemlog.h"
#include "libpmemblk.h"
#include "pmempool.h"
#include "check_util.h"

/*
 * pool_get_min_size -- return minimum size of pool for specified type
 */
static uint64_t
pool_get_min_size(enum pool_type type)
{
	switch (type) {
	case POOL_TYPE_LOG:
		return PMEMLOG_MIN_POOL;
	case POOL_TYPE_BLK:
		return PMEMBLK_MIN_POOL;
	case POOL_TYPE_OBJ:
		return PMEMOBJ_MIN_POOL;
	default:
		break;
	}

	return 0;
}

/*
 * pool_set_map -- map poolset
 */
static int
pool_set_map(const char *fname, struct pool_set **poolset, int rdonly)
{
	if (util_is_poolset(fname) != 1)
		return util_pool_open_nocheck(poolset, fname, rdonly);

	int fd = util_file_open(fname, NULL, 0, O_RDONLY);
	if (fd < 0)
		return -1;

	int ret = 0;

	struct pool_set *set = NULL;

	/* parse poolset file */
	if (util_poolset_parse(fname, fd, &set)) {
		ERR("parsing poolset file failed");
		ret = -1;
		goto err_close;
	}

	/* open the first part set file to read the pool header values */
	int fdp = util_file_open(set->replica[0]->part[0].path,
			NULL, 0, O_RDONLY);
	if (fdp < 0) {
		ERR("cannot open poolset part file");
		ret = -1;
		goto err_pool_set;
	}

	struct pool_hdr hdr;
	/* read the pool header from first pool set file */
	if (pread(fdp, &hdr, sizeof (hdr), 0)
			!= sizeof (hdr)) {
		ERR("cannot read pool header from poolset");
		ret = -1;
		goto err_close_part;
	}

	close(fdp);
	util_poolset_free(set);
	close(fd);

	pool_hdr_convert2h(&hdr);

	/* parse pool type from first pool set file */
	enum pool_type type = pool_hdr_get_type(&hdr);
	if (type == POOL_TYPE_UNKNOWN) {
		ERR("cannot determine pool type from poolset");
		return -1;
	}

	/* get minimum size based on pool type for util_pool_open */
	size_t minsize = pool_get_min_size(type);

	/*
	 * Open the poolset, the values passed to util_pool_open are read
	 * from the first poolset file, these values are then compared with
	 * the values from all headers of poolset files.
	 */
	if (util_pool_open(poolset, fname, rdonly, minsize,
			hdr.signature, hdr.major,
			hdr.compat_features,
			hdr.incompat_features,
			hdr.ro_compat_features)) {
		ERR("openning poolset failed\n");
		return -1;
	}

	return 0;
err_close_part:
	close(fdp);
err_pool_set:
	util_poolset_free(set);
err_close:
	close(fd);

	return ret;
}

/*
 * pool_parse_params -- parse pool type, file size and block size
 */
static int
pool_params_parse(const PMEMpoolcheck *ppc, struct pool_params *params,
	int check)
{
	struct stat stat_buf;
	int ret = 0;

	params->type = POOL_TYPE_UNKNOWN;

	params->is_poolset = util_is_poolset(ppc->path) == 1;
	int fd = util_file_open(ppc->path, NULL, 0, O_RDONLY);
	if (fd < 0)
		return -1;

	/* get file size and mode */
	if (fstat(fd, &stat_buf)) {
		ret = -1;
		goto out_close;
	}

	ASSERT(stat_buf.st_size >= 0);
	params->size = (uint64_t)stat_buf.st_size;
	params->mode = stat_buf.st_mode;

	void *addr = NULL;
	struct pool_set *set = NULL;
	if (params->is_poolset) {
		/* close the file */
		close(fd);
		fd = -1;

		if (check) {
			if (pool_set_map(ppc->path, &set, 1))
				return -1;
		} else {
			if (util_pool_open_nocheck(&set, ppc->path, 1))
				return -1;
		}

		params->size = set->poolsize;
		addr = set->replica[0]->part[0].addr;
	} else {
		addr = mmap(NULL, (uint64_t)stat_buf.st_size,
				PROT_READ, MAP_PRIVATE, fd, 0);
		if (addr == MAP_FAILED) {
			ret = -1;
			goto out_close;
		}
	}

	if (ppc->pool_type != PMEMPOOL_POOL_TYPE_BTT_DEV) {
		struct pool_hdr hdr;
		memcpy(&hdr, addr, sizeof (hdr));

		pool_hdr_convert2h(&hdr);

		memcpy(params->signature, hdr.signature,
			sizeof (params->signature));

		/*
		 * Check if file is a part of pool set by comparing
		 * the UUID with the next part UUID. If it is the same
		 * it means the pool consist of a single file.
		 */
		params->is_part = !params->is_poolset &&
			(memcmp(hdr.uuid, hdr.next_part_uuid,
			POOL_HDR_UUID_LEN) || memcmp(hdr.uuid,
			hdr.prev_part_uuid, POOL_HDR_UUID_LEN));

		params->type = pool_hdr_get_type(&hdr);

		enum pool_type declared_type = 1 << ppc->pool_type;
		if ((params->type & ~declared_type) != 0) {
			ERR("declared pool type does not match");
			ret = -1;
			goto out_close;
		}

		if (params->type == POOL_TYPE_BLK) {
			struct pmemblk pbp;
			memcpy(&pbp, addr, sizeof (pbp));
			params->blk.bsize = le32toh(pbp.bsize);
		} else if (params->type == POOL_TYPE_OBJ) {
			struct pmemobjpool pop;
			memcpy(&pop, addr, sizeof (pop));
			memcpy(params->obj.layout, pop.layout,
				PMEMOBJ_MAX_LAYOUT);
		}
	}

	if (params->is_poolset)
		util_poolset_close(set, 0);
	else
		munmap(addr, (uint64_t)stat_buf.st_size);
out_close:
	if (fd >= 0)
		(void) close(fd);
	return ret;
}

/*
 * pool_set_file_open -- opens pool set file or regular file
 */
static struct pool_set_file *
pool_set_file_open(const char *fname, int rdonly, int check)
{
	struct pool_set_file *file = calloc(1, sizeof (*file));
	if (!file)
		return NULL;

	file->replica = 0;
	file->fname = strdup(fname);
	if (!file->fname)
		goto err;

	struct stat buf;

	/*
	 * The check flag indicates whether the headers from each pool
	 * set file part should be checked for valid values.
	 */
	if (check) {
		if (pool_set_map(file->fname,
				&file->poolset, rdonly))
			goto err_free_fname;
	} else {
		if (util_pool_open_nocheck(&file->poolset, file->fname,
				rdonly))
			goto err_free_fname;
	}

	file->size = file->poolset->poolsize;

	/* get modification time from the first part of first replica */
	const char *path = file->poolset->replica[0]->part[0].path;
	if (stat(path, &buf)) {
		ERR("%s", path);
		goto err_close_poolset;
	}

	file->mtime = buf.st_mtime;
	file->mode = buf.st_mode;
	file->addr = file->poolset->replica[0]->part[0].addr;

	return file;

err_close_poolset:
	util_poolset_close(file->poolset, 0);
err_free_fname:
	free(file->fname);
err:
	free(file);
	return NULL;
}

/*
 * pool_data_alloc -- allocate pool data and open set_file
 */
struct pool_data *
pool_data_alloc(PMEMpoolcheck *ppc)
{
	struct pool_data *pool = malloc(sizeof (*pool));
	if (!pool) {
		ERR("!malloc");
		return NULL;
	}

	TAILQ_INIT(&pool->arenas);
	pool->narenas = 0;

	if (pool_params_parse(ppc, &pool->params, 0)) {
		if (errno)
			perror(ppc->path);
		else
			ERR("%s: cannot determine type of pool\n",
				ppc->path);
		goto error;
	}

	int rdonly = !ppc->repair || ppc->dry_run;
	ppc->pool->set_file = pool_set_file_open(ppc->path, rdonly, 0);
	if (!ppc->pool->set_file) {
		perror(ppc->path);
		goto error;
	}

	return pool;

error:
	pool_data_free(pool);
	return NULL;
}

/*
 * pool_set_file_close -- closes pool set file or regular file
 */
static void
pool_set_file_close(struct pool_set_file *file)
{
	if (file->poolset)
		util_poolset_close(file->poolset, 0);
	else if (file->addr) {
		munmap(file->addr, file->size);
		close(file->fd);
	}
	free(file->fname);
	free(file);
}

/*
 * pool_data_free -- close set_file and release pool data
 */
void
pool_data_free(struct pool_data *pool)
{
	if (pool->set_file)
		pool_set_file_close(pool->set_file);

	while (!TAILQ_EMPTY(&pool->arenas)) {
		struct arena *arenap = TAILQ_FIRST(&pool->arenas);
		if (arenap->map)
			free(arenap->map);
		if (arenap->flog)
			free(arenap->flog);
		TAILQ_REMOVE(&pool->arenas, arenap, next);
		free(arenap);
	}

	free(pool);
}

/*
 * pool_set_file_map -- return mapped address at given offset
 */
void *
pool_set_file_map(struct pool_set_file *file, uint64_t offset)
{
	if (file->addr == MAP_FAILED)
		return NULL;
	return (char *)file->addr + offset;
}

/*
 * pool_read -- read from pool set file or regular file
 */
int
pool_read(struct pool_set_file *file, void *buff, size_t nbytes, uint64_t off)
{
	if (off + nbytes > file->size)
		return -1;

	memcpy(buff, (char *)file->addr + off, nbytes);

	return 0;
}

/*
 * pool_write -- write to pool set file or regular file
 */
int
pool_write(struct pool_set_file *file, void *buff, size_t nbytes, uint64_t off)
{
	if (off + nbytes > file->size)
		return -1;

	memcpy((char *)file->addr + off, buff, nbytes);

	return 0;
}

/*
 * pool_set_file_map_headers -- map headers of each pool set part file
 */
int
pool_set_file_map_headers(struct pool_set_file *file, int rdonly,
	size_t hdrsize)
{
	if (!file->poolset)
		return -1;

	int flags = rdonly ? MAP_PRIVATE : MAP_SHARED;
	for (unsigned r = 0; r < file->poolset->nreplicas; r++) {
		struct pool_replica *rep = file->poolset->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];

			part->hdr = mmap(NULL, hdrsize, PROT_READ | PROT_WRITE,
					flags, part->fd, 0);
			if (part->hdr == MAP_FAILED) {
				part->hdr = NULL;
				goto err;
			}

			part->hdrsize = hdrsize;
		}
	}

	return 0;
err:
	pool_set_file_unmap_headers(file);
	return -1;
}

/*
 * pool_set_file_unmap_headers -- unmap headers of each pool set part file
 */
void
pool_set_file_unmap_headers(struct pool_set_file *file)
{
	if (!file->poolset)
		return;
	for (unsigned r = 0; r < file->poolset->nreplicas; r++) {
		struct pool_replica *rep = file->poolset->replica[r];
		for (unsigned p = 0; p < rep->nparts; p++) {
			struct pool_set_part *part = &rep->part[p];
			if (part->hdr != NULL) {
				ASSERT(part->hdrsize > 0);
				munmap(part->hdr, part->hdrsize);
				part->hdr = NULL;
				part->hdrsize = 0;
			}
		}
	}
}

/*
 * pool_hdr_convert2h -- convert pool header to host byte order
 */
void
pool_hdr_convert2h(struct pool_hdr *hdrp)
{
	hdrp->compat_features = le32toh(hdrp->compat_features);
	hdrp->incompat_features = le32toh(hdrp->incompat_features);
	hdrp->ro_compat_features = le32toh(hdrp->ro_compat_features);
	hdrp->arch_flags.alignment_desc =
		le64toh(hdrp->arch_flags.alignment_desc);
	hdrp->arch_flags.e_machine = le16toh(hdrp->arch_flags.e_machine);
	hdrp->crtime = le64toh(hdrp->crtime);
	hdrp->checksum = le64toh(hdrp->checksum);
}

/*
 * pool_hdr_convert2le -- convert pool header to LE byte order
 */
void
pool_hdr_convert2le(struct pool_hdr *hdrp)
{
	hdrp->compat_features = htole32(hdrp->compat_features);
	hdrp->incompat_features = htole32(hdrp->incompat_features);
	hdrp->ro_compat_features = htole32(hdrp->ro_compat_features);
	hdrp->arch_flags.alignment_desc =
		htole64(hdrp->arch_flags.alignment_desc);
	hdrp->arch_flags.e_machine = htole16(hdrp->arch_flags.e_machine);
	hdrp->crtime = htole64(hdrp->crtime);
	hdrp->checksum = htole64(hdrp->checksum);
}

/*
 * pool_get_signature -- return signature of specified pool type
 */
static const char *
pool_get_signature(enum pool_type type)
{
	switch (type) {
	case POOL_TYPE_LOG:
		return LOG_HDR_SIG;
	case POOL_TYPE_BLK:
		return BLK_HDR_SIG;
	case POOL_TYPE_OBJ:
		return OBJ_HDR_SIG;
	default:
		return NULL;
	}
}

/*
 * pool_hdr_default -- return default pool header values
 */
void
pool_hdr_default(enum pool_type type, struct pool_hdr *hdrp)
{
	memset(hdrp, 0, sizeof (*hdrp));
	const char *sig = pool_get_signature(type);
	ASSERTne(sig, NULL);

	memcpy(hdrp->signature, sig, POOL_HDR_SIG_LEN);

	switch (type) {
	case POOL_TYPE_LOG:
		hdrp->major = LOG_FORMAT_MAJOR;
		hdrp->compat_features = LOG_FORMAT_COMPAT;
		hdrp->incompat_features = LOG_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = LOG_FORMAT_RO_COMPAT;
		break;
	case POOL_TYPE_BLK:
		hdrp->major = BLK_FORMAT_MAJOR;
		hdrp->compat_features = BLK_FORMAT_COMPAT;
		hdrp->incompat_features = BLK_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = BLK_FORMAT_RO_COMPAT;
		break;
	case POOL_TYPE_OBJ:
		hdrp->major = OBJ_FORMAT_MAJOR;
		hdrp->compat_features = OBJ_FORMAT_COMPAT;
		hdrp->incompat_features = OBJ_FORMAT_INCOMPAT;
		hdrp->ro_compat_features = OBJ_FORMAT_RO_COMPAT;
		break;
	default:
		break;
	}
}

/*
 * pool_hdr_get_type -- return pool type based on pool header data
 */
enum pool_type
pool_hdr_get_type(const struct pool_hdr *hdrp)
{
	if (memcmp(hdrp->signature, LOG_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return POOL_TYPE_LOG;
	else if (memcmp(hdrp->signature, BLK_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return POOL_TYPE_BLK;
	else if (memcmp(hdrp->signature, OBJ_HDR_SIG, POOL_HDR_SIG_LEN) == 0)
		return POOL_TYPE_OBJ;
	else
		return POOL_TYPE_UNKNOWN;
}

/*
 * pool_btt_info_convert2h -- convert btt_info header to host byte order
 */
void
pool_btt_info_convert2h(struct btt_info *infop)
{
	infop->flags = le32toh(infop->flags);
	infop->minor = le16toh(infop->minor);
	infop->external_lbasize = le32toh(infop->external_lbasize);
	infop->external_nlba = le32toh(infop->external_nlba);
	infop->internal_lbasize = le32toh(infop->internal_lbasize);
	infop->internal_nlba = le32toh(infop->internal_nlba);
	infop->nfree = le32toh(infop->nfree);
	infop->infosize = le32toh(infop->infosize);
	infop->nextoff = le64toh(infop->nextoff);
	infop->dataoff = le64toh(infop->dataoff);
	infop->mapoff = le64toh(infop->mapoff);
	infop->flogoff = le64toh(infop->flogoff);
	infop->infooff = le64toh(infop->infooff);
	infop->checksum = le64toh(infop->checksum);
}

#define	BTT_INFO_SIG	"BTT_ARENA_INFO\0"

/*
 * pool_btt_info_valid -- check consistency of BTT Info header
 */
int
pool_btt_info_valid(struct btt_info *infop)
{
	if (!memcmp(infop->sig, BTT_INFO_SIG, BTTINFO_SIG_LEN))
		return util_checksum(infop, sizeof (*infop), &infop->checksum,
			0);
	else
		return -1;
}

/*
 * pool_get_first_valid_arena -- get first valid BTT Info in arena
 */
int
pool_get_first_valid_arena(struct pool_set_file *file, struct arena *arenap)
{
	uint64_t offset = 2 * BTT_ALIGNMENT;
	int backup = 0;
	struct btt_info *infop = &arenap->btt_info;
	arenap->zeroed = true;

	uint64_t last_offset = (file->size & ~(BTT_ALIGNMENT - 1))
			- BTT_ALIGNMENT;
	/*
	 * Starting at offset, read every page and check for
	 * valid BTT Info Header. Check signature and checksum.
	 */
	while (!pool_read(file, infop, sizeof (*infop), offset)) {
		bool zeroed = !check_memory((const uint8_t *)infop,
				sizeof (*infop), 0);
		arenap->zeroed = arenap->zeroed && zeroed;

		if (pool_btt_info_valid(infop)) {
			pool_btt_info_convert2h(infop);
			arenap->valid = true;
			arenap->offset = offset;
			return 1;
		}

		if (!backup) {
			if (file->size > BTT_MAX_ARENA)
				offset += BTT_MAX_ARENA - BTT_ALIGNMENT;
			else
				offset = last_offset;
		} else {
			offset += BTT_ALIGNMENT;
		}

		backup = !backup;
	}

	return 0;
}

/*
 * pool_get_first_valid_btt -- return offset to first valid BTT Info
 *
 * - Return offset to first valid BTT Info header in pool file.
 * - Start at specific offset.
 * - Convert BTT Info header to host endianness.
 * - Return the BTT Info header by pointer.
 */
uint64_t
pool_get_first_valid_btt(struct pmempool_check *ppc, struct btt_info
	*infop, uint64_t offset)
{
	/*
	 * Starting at offset, read every page and check for
	 * valid BTT Info Header. Check signature and checksum.
	 */
	while (!pool_read(ppc->pool->set_file, infop, sizeof (*infop),
		offset)) {

		if (pool_btt_info_valid(infop)) {
			pool_btt_info_convert2h(infop);
			return offset;
		}

		offset += BTT_ALIGNMENT;
	}

	return 0;
}