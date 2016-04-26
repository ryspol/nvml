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
 * check_backup.c -- pre-check backup
 */

#include <unistd.h>
#include <stdint.h>
#include <sys/mman.h>

#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"
#include "check_backup.h"

/*
 * backup_create -- create backup file
 */
static int
backup_create(PMEMpoolcheck *ppc)
{
	CHECK_INFO(ppc, "creating backup file: %s", ppc->backup_path);

	struct pool_set_file *file = ppc->pool->set_file;
	int dfd = util_file_create(ppc->backup_path, file->size, 0);
	if (dfd < 0)
		return -1;

	void *daddr = mmap(NULL, file->size, PROT_READ | PROT_WRITE,
		MAP_SHARED, dfd, 0);
	if (daddr == MAP_FAILED) {
		close(dfd);
		return -1;
	}

	void *saddr = pool_set_file_map(file, 0);

	memcpy(daddr, saddr, file->size);
	munmap(daddr, file->size);
	close(dfd);

	return 0;
}

/*
 * check_backup -- perform backup if requested and needed
 */
void
check_backup(PMEMpoolcheck *ppc)
{
	if (ppc->args.repair && ppc->backup_path != NULL &&
		!ppc->args.dry_run) {
		if (backup_create(ppc)) {
			ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
			CHECK_ERR(ppc, "unable to create backup file");
		}
	}
}
